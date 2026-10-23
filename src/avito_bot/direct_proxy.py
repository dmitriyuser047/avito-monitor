"""Локальный HTTP CONNECT-прокси, который выходит в интернет напрямую через
домашнюю сеть, минуя VPN (Clash, sing-box и т.п. в режиме TUN).

VPN перехватывает весь трафик и DNS (отдаёт фейковые адреса 198.18.x.x),
поэтому мы:
  * узнаём настоящий IP через DNS-over-HTTPS;
  * привязываем исходящий сокет к домашней сети:
      - DIRECT_INTERFACE=en0 (имя интерфейса): macOS — IP_BOUND_IF, Linux — SO_BINDTODEVICE;
      - DIRECT_INTERFACE=192.168.1.5 (локальный IP): исходящий адрес — работает и на Windows.
Браузер Playwright ходит через этот прокси, и Авито видит домашний IP.
"""

import asyncio
import ipaddress
import logging
import socket
import sys
import time

import httpx

log = logging.getLogger(__name__)

IP_BOUND_IF = 25  # macOS: setsockopt(IPPROTO_IP, IP_BOUND_IF, ifindex)
SO_BINDTODEVICE = 25  # Linux
DOH_URL = "https://dns.google/resolve"
DNS_TTL = 300


class DirectProxy:
    def __init__(self, interface: str = "en0", host: str = "127.0.0.1", port: int = 8899):
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
        self._http = httpx.AsyncClient(timeout=10)
        self._server: asyncio.base_events.Server | None = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        log.info("Прямой прокси %s → %s", self.url, self.interface)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        await self._http.aclose()

    async def resolve(self, name: str) -> str:
        cached = self._dns.get(name)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        r = await self._http.get(DOH_URL, params={"name": name, "type": "A"})
        answers = [a["data"] for a in r.json().get("Answer", []) if a.get("type") == 1]
        if not answers:
            raise OSError(f"DNS: нет A-записи для {name}")
        self._dns[name] = (answers[0], time.monotonic() + DNS_TTL)
        return answers[0]

    async def _open_direct(self, host: str, port: int):
        ip = await self.resolve(host)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self._source_ip:
            sock.bind((self._source_ip, 0))
        elif sys.platform == "darwin":
            sock.setsockopt(socket.IPPROTO_IP, IP_BOUND_IF, self._ifindex)
        else:
            sock.setsockopt(socket.SOL_SOCKET, SO_BINDTODEVICE, self.interface.encode())
        sock.setblocking(False)
        await asyncio.get_running_loop().sock_connect(sock, (ip, port))
        return await asyncio.open_connection(sock=sock)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            method, target, _ = head.split(b"\r\n", 1)[0].decode().split(" ", 2)
            if method != "CONNECT":
                writer.write(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                await writer.drain()
                return
            host, port = target.rsplit(":", 1)
            up_reader, up_writer = await self._open_direct(host, int(port))
        except Exception as e:
            log.debug("Прокси: ошибка подключения: %s", e)
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            await _close(writer)
            return

        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await writer.drain()
        await asyncio.gather(_pipe(reader, up_writer), _pipe(up_reader, writer))


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
        await writer.wait_closed()
    except Exception:
        pass
