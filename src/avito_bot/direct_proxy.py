"""Локальный HTTP CONNECT-прокси, который выходит в интернет напрямую через
домашнюю сеть, минуя VPN (Clash, sing-box и т.п. в режиме TUN).

VPN перехватывает весь трафик и DNS (отдаёт фейковые адреса 198.18.x.x),
поэтому мы:
  * узнаём настоящий IP через DNS-over-HTTPS;
  * привязываем исходящий сокет к домашней сети:
      - DIRECT_INTERFACE=en0 (имя интерфейса): macOS — IP_BOUND_IF, Linux — SO_BINDTODEVICE;
      - DIRECT_INTERFACE=192.168.1.5 (локальный IP): исходящий адрес — работает и на Windows.
Маршрут CONNECT и DoH привязан к интерфейсу; внешний IP без измерения неизвестен.
"""

import asyncio
import ipaddress
import logging
import socket
import sys
import time

import httpx
import httpcore

log = logging.getLogger(__name__)

IP_BOUND_IF = 25  # macOS: setsockopt(IPPROTO_IP, IP_BOUND_IF, ifindex)
SO_BINDTODEVICE = 25  # Linux
DOH_URL = "https://dns.google/resolve"
DNS_TTL = 300


class _BoundStream(httpcore.AsyncNetworkStream):
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    async def read(self, max_bytes, timeout=None):
        return await asyncio.wait_for(self.reader.read(max_bytes), timeout)

    async def write(self, buffer, timeout=None):
        self.writer.write(buffer)
        await asyncio.wait_for(self.writer.drain(), timeout)

    async def aclose(self):
        await _close(self.writer)

    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        try:
            await asyncio.wait_for(self.writer.start_tls(ssl_context, server_hostname=server_hostname), timeout)
        except BaseException:
            await self.aclose()
            raise
        return self

    def get_extra_info(self, info):
        if info == 'is_readable':
            return self.reader.at_eof() or bool(self.reader._buffer)
        return self.writer.get_extra_info({'client_addr': 'sockname', 'server_addr': 'peername'}.get(info, info))


class _DNSBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, proxy):
        self.proxy = proxy

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        if host != 'dns.google' or port != 443:
            raise OSError('Unexpected DoH destination')
        # Fixed resolver bootstrap avoids an OS/VPN DNS lookup for the DoH provider itself.
        reader, writer = await self.proxy._open_bound('8.8.8.8', port)
        return _BoundStream(reader, writer)

    async def sleep(self, seconds):
        await asyncio.sleep(seconds)


class _DoHClient:
    def __init__(self, proxy):
        self.pool = httpcore.AsyncConnectionPool(network_backend=_DNSBackend(proxy),
                                                max_connections=4, max_keepalive_connections=2)

    async def get(self, url, params):
        request = httpx.Request('GET', url, params=params)
        response = await self.pool.request('GET', str(request.url),
            headers={'Accept': 'application/dns-json'},
            extensions={'timeout': dict(connect=10, read=10, write=10, pool=10)})
        return httpx.Response(response.status, headers=response.headers, content=response.content, request=request)

    async def aclose(self):
        await self.pool.aclose()


class DirectProxy:
    def __init__(self, interface: str = "en0", host: str = "127.0.0.1", port: int = 8899, max_connections: int = 128):
        if host != '127.0.0.1':
            raise ValueError('DirectProxy must listen on 127.0.0.1')
        self.max_connections = max_connections
        self._connections = set()
        self._stopped = False
        self.last_error = None
        self.interface = interface
        self.host = host
        self.port = port
        try:
            self._source_ip = str(ipaddress.ip_address(interface))
        except ValueError:
            self._source_ip = None
            if sys.platform == "win32":
                raise ValueError("На Windows укажите в DIRECT_INTERFACE локальный IP компьютера, например 192.168.1.5")
            self._ifindex = socket.if_nametoindex(interface)  # заодно проверка, что интерфейс есть
        self._dns: dict[str, tuple[str, float]] = {}
        self._resolving: dict[str, asyncio.Task] = {}
        # A cold profile asks for dozens of names at once; keep DoH to a few sockets.
        self._http = _DoHClient(self)
        self._server: asyncio.base_events.Server | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def start(self) -> None:
        if self._server and self._server.is_serving():
            raise RuntimeError('Proxy already started')
        if self._stopped:
            self._http = _DoHClient(self)
        self._stopped = False
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("Прямой прокси %s → %s", self.url, self.interface)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
        server, self._server = self._server, None
        tasks = list(self._connections | set(self._resolving.values()))
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if server:
            await server.wait_closed()
        self._connections.clear()
        self._resolving.clear()
        await self._http.aclose()
        self._stopped = True

    def health(self):
        return dict(route='proxy', interface=self.interface, dns_route='same_bound_interface',
                    external_ip='unknown', active_connections=len(self._connections),
                    connected=bool(self._server and self._server.is_serving()), last_error=self.last_error)

    async def resolve(self, name: str) -> str:
        cached = self._dns.get(name)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        # Parallel tunnels to one host share a single DoH request.
        task = self._resolving.get(name)
        if task is None:
            task = self._resolving[name] = asyncio.ensure_future(self._query(name))
            task.add_done_callback(lambda t: (self._resolving.pop(name, None), t.cancelled() or t.exception()))
        return await asyncio.shield(task)

    async def _query(self, name: str) -> str:
        r = await self._http.get(DOH_URL, params={"name": name, "type": "A"})
        r.raise_for_status()
        answers = [a["data"] for a in r.json().get("Answer", []) if a.get("type") == 1]
        if not answers:
            raise OSError(f"DNS: нет A-записи для {name}")
        self._dns[name] = (answers[0], time.monotonic() + DNS_TTL)
        return answers[0]

    async def _open_direct(self, host: str, port: int):
        ip = await self.resolve(host)
        return await self._open_bound(ip, port)

    async def _open_bound(self, ip: str, port: int):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if self._source_ip:
                sock.bind((self._source_ip, 0))
            elif sys.platform == "darwin":
                sock.setsockopt(socket.IPPROTO_IP, IP_BOUND_IF, self._ifindex)
            else:
                sock.setsockopt(socket.SOL_SOCKET, SO_BINDTODEVICE, self.interface.encode())
            sock.setblocking(False)
            await asyncio.wait_for(asyncio.get_running_loop().sock_connect(sock, (ip, port)), 15)
            return await asyncio.open_connection(sock=sock)
        except BaseException:
            sock.close()
            raise

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if len(self._connections) >= self.max_connections:
            writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
            await _close(writer)
            return
        self._connections.add(task)
        up_writer = None
        connected = False
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            method, target, _ = head.split(b"\r\n", 1)[0].decode().split(" ", 2)
            if method != "CONNECT":
                writer.write(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                await writer.drain()
                return
            host, port = target.rsplit(":", 1)
            up_reader, up_writer = await self._open_direct(host, int(port))
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()
            connected = True
            await asyncio.gather(_pipe(reader, up_writer), _pipe(up_reader, writer))
        except Exception as exc:
            self.last_error = type(exc).__name__
            log.debug("Proxy connection failed: %s", self.last_error)
            if not connected and not writer.is_closing():
                try:
                    writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await writer.drain()
                except ConnectionError:
                    pass
        finally:
            self._connections.discard(task)
            await _close(writer)
            if up_writer:
                await _close(up_writer)


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        await _close(writer)


async def _close(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await asyncio.wait_for(writer.wait_closed(), 2)
    except Exception:
        pass
