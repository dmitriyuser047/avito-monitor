import asyncio
import socket
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from avito_bot.direct_proxy import DirectProxy
from avito_bot.runtime import InstanceLock
from avito_bot.supervisor import restart_delay


def test_restart_limit_and_terminal_exit_codes():
    for code in (0,78,130,-2,-15):
        assert restart_delay(code,[],100) is None
    assert restart_delay(1,[],100) == 10
    assert restart_delay(1,[90,95],100) == 60
    assert restart_delay(1,[90,91,92,93,94],100) is None
    assert restart_delay(1,[1,2,3,4,5],2000) == 10


def test_os_lock_prevents_duplicate_instance_and_releases(tmp_path):
    path=tmp_path/'lock'
    code='from pathlib import Path; from avito_bot.runtime import InstanceLock; lock=InstanceLock(Path(__import__("sys").argv[1])); lock.__enter__()'
    with InstanceLock(path):
        result=subprocess.run([sys.executable,'-c',code,str(path)],capture_output=True)
        assert result.returncode != 0 and b'BlockingIOError' in result.stderr
    result=subprocess.run([sys.executable,'-c',code,str(path)],capture_output=True)
    assert result.returncode == 0


@pytest.mark.parametrize('response', [
    httpx.Response(200,json={'Answer':[]},request=httpx.Request('GET','https://dns.google/resolve')),
    httpx.Response(503,request=httpx.Request('GET','https://dns.google/resolve')),
])
def test_dns_failure_is_explicit_and_not_cached(response):
    async def scenario():
        proxy=DirectProxy('127.0.0.1',port=0)
        proxy._http.get=AsyncMock(return_value=response)
        with pytest.raises((OSError,httpx.HTTPStatusError)):
            await proxy.resolve('fixture.test')
        assert not proxy._dns
        await proxy.stop()
    asyncio.run(scenario())


def test_dns_timeout_is_propagated():
    async def scenario():
        proxy=DirectProxy('127.0.0.1',port=0)
        proxy._http.get=AsyncMock(side_effect=httpx.ReadTimeout('timeout'))
        with pytest.raises(httpx.ReadTimeout):
            await proxy.resolve('fixture.test')
        await proxy.stop()
    asyncio.run(scenario())


def test_socket_closed_on_connect_error_and_bound_to_selected_ip(monkeypatch):
    async def scenario():
        proxy=DirectProxy('127.0.0.1',port=0)
        proxy.resolve=AsyncMock(return_value='127.0.0.1')
        sock=Mock()
        loop=asyncio.get_running_loop()
        monkeypatch.setattr('avito_bot.direct_proxy.socket.socket',lambda *args: sock)
        monkeypatch.setattr(loop,'sock_connect',AsyncMock(side_effect=TimeoutError()))
        with pytest.raises(TimeoutError):
            await proxy._open_direct('fixture.test',443)
        sock.bind.assert_called_once_with(('127.0.0.1',0))
        sock.close.assert_called_once()
        await proxy.stop()
    asyncio.run(scenario())


def test_direct_proxy_uses_real_selected_loopback_connection():
    async def scenario():
        proxy=DirectProxy('127.0.0.1',port=0)
        proxy.resolve=AsyncMock(return_value='127.0.0.1')
        peers=[]
        async def server(reader,writer):
            peers.append(writer.get_extra_info('peername')[0])
            writer.write(b'ok');await writer.drain();writer.close();await writer.wait_closed()
        listener=await asyncio.start_server(server,'127.0.0.1',0)
        reader,writer=await proxy._open_direct('fixture.test',listener.sockets[0].getsockname()[1])
        assert await reader.read()==b'ok'
        assert peers==['127.0.0.1']
        writer.close();await writer.wait_closed()
        listener.close();await listener.wait_closed();await proxy.stop()
    asyncio.run(scenario())


def test_deployment_backup_and_plist_are_private_and_portable(tmp_path):
    import sqlite3
    from avito_bot.service import backup_database, launch_agent
    db=tmp_path/'bot.sqlite3'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE saved(value TEXT)')
        conn.execute("INSERT INTO saved VALUES('preserve')")
    backup=backup_database(db,tmp_path/'backups')
    with sqlite3.connect(backup) as conn:
        assert conn.execute('SELECT value FROM saved').fetchone()[0]=='preserve'
    assert backup.stat().st_mode & 0o777 == 0o600
    plist=launch_agent(tmp_path,'/fixture/python')
    assert plist['KeepAlive'] is False
    assert plist['ProgramArguments'][0]=='/fixture/python'
    assert 'BOT_TOKEN' not in str(plist)


def test_setup_owner_never_claimed_by_first_start(tmp_path):
    from aiogram.types import Update, Message, Chat, User, MessageEntity
    from avito_bot.bot import TelegramBot
    from avito_bot.db import DB
    from datetime import datetime
    async def scenario():
        db=DB(tmp_path/'owner.sqlite3')
        tg=TelegramBot('123456:TEST-token',db,None)
        tg.bot.session.make_request=AsyncMock(return_value=True)
        update=Update(update_id=1,message=Message(message_id=1,date=datetime.now(),
                      chat=Chat(id=999,type='private'),from_user=User(id=999,is_bot=False,first_name='stranger'),
                      text='/start',entities=[MessageEntity(type='bot_command',offset=0,length=6)]))
        await tg.dp.feed_update(tg.bot,update)
        assert tg.owner_id is None and db.get('owner_id') is None
        assert not tg.allowed(999)
        db.set('owner_id','123')
        legacy=TelegramBot('123456:TEST-token',db,None)
        assert legacy.owner_id==123 and legacy.allowed(123)
        await legacy.bot.session.close();await tg.bot.session.close();db.close()
    asyncio.run(scenario())


def test_source_startup_failure_keeps_telegram_and_worker_independent_and_closes_all(tmp_path, monkeypatch):
    import avito_bot
    from avito_bot.config import load_config
    cfg=load_config();cfg.data_dir=tmp_path;cfg.bot_token='123456:TEST-token';cfg.owner_id=1;cfg.direct_interface=None
    monkeypatch.setattr(avito_bot,'load_config',lambda:cfg)
    events=[]
    class FakeScraper:
        source_id='local_browser'
        closed=False
        def __init__(self,*args):pass
        async def start(self):raise OSError('browser unavailable')
        async def stop(self):events.append('browser closed')
        async def download(self,url):return None
    class FakeTelegram:
        owner_id=1
        def __init__(self,*args):
            self.bot=SimpleNamespace(session=SimpleNamespace(close=AsyncMock()))
            self.dp=SimpleNamespace(start_polling=self.poll)
            self.alert=AsyncMock()
            instances.append(self)
        async def setup(self):pass
        def recipients(self):return [1]
        def allowed(self,uid):return uid==1
        async def send_to_chat(self,*args):pass
        async def poll(self,*args,**kwargs):
            events.append('telegram started')
            try:await asyncio.Event().wait()
            finally:events.append('telegram closed')
    class FakeWorker:
        def __init__(self,*args):pass
        async def run(self):
            events.append('delivery started')
            try:await asyncio.Event().wait()
            finally:events.append('delivery closed')
    instances=[]
    monkeypatch.setattr(avito_bot,'Scraper',FakeScraper)
    monkeypatch.setattr(avito_bot,'TelegramBot',FakeTelegram)
    monkeypatch.setattr(avito_bot,'DeliveryWorker',FakeWorker)
    async def run_monitor(self):
        await asyncio.sleep(.01)
        assert self._source_stopped and 'STOPPED' in self.status
        assert 'telegram started' in events and 'delivery started' in events
    monkeypatch.setattr(avito_bot.Monitor,'run',run_monitor)
    asyncio.run(avito_bot.run())
    assert 'browser closed' in events and 'telegram closed' in events and 'delivery closed' in events
    instances[0].bot.session.close.assert_awaited_once()


def test_service_installation_waits_for_old_instance_cleanup(tmp_path,monkeypatch):
    from avito_bot import service
    calls=[]
    class FinishingLock:
        def __init__(self,path):pass
        def __enter__(self):
            calls.append('enter')
            if calls.count('enter')<3:raise BlockingIOError()
        def __exit__(self,*args):calls.append('exit')
    monkeypatch.setattr(service,'InstanceLock',FinishingLock)
    monkeypatch.setattr(service.time,'sleep',lambda _:None)
    with service.installation_lock(tmp_path/'lock'):
        calls.append('installed')
    assert calls==['enter','enter','enter','installed','exit']


def test_service_installation_timeout_preserves_running_instance(tmp_path,monkeypatch):
    from avito_bot import service
    class RunningLock:
        def __init__(self,path):pass
        def __enter__(self):raise BlockingIOError()
    monkeypatch.setattr(service,'InstanceLock',RunningLock)
    with pytest.raises(RuntimeError,match='still holds the lock'):
        with service.installation_lock(tmp_path/'lock',timeout=0):
            pytest.fail('Must not install over a running process')


def test_service_waits_until_launchd_removes_previous_job(monkeypatch):
    from avito_bot import service
    results=iter([SimpleNamespace(returncode=0),SimpleNamespace(returncode=0),SimpleNamespace(returncode=1)])
    calls=[]
    def run(command,**kwargs):
        calls.append(command)
        return next(results)
    monkeypatch.setattr(service.subprocess,'run',run)
    monkeypatch.setattr(service.time,'sleep',lambda _:None)
    service.wait_for_unload('gui/501/test-job')
    assert calls==[['launchctl','print','gui/501/test-job']]*3


def test_service_unload_timeout_prevents_bootstrap(monkeypatch):
    from avito_bot import service
    monkeypatch.setattr(service.subprocess,'run',lambda *args,**kwargs:SimpleNamespace(returncode=0))
    with pytest.raises(RuntimeError,match='still unloading'):
        service.wait_for_unload('gui/501/test-job',timeout=0)
