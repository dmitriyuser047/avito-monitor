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
    # Repeated crashes slow down to 10 minutes but never stop the service for good.
    assert restart_delay(1,[90,91,92,93,94],100) == 600
    assert restart_delay(1,list(range(50)),100) == 600
    assert restart_delay(75,[],100) == 10  # watchdog exit
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


def test_concurrent_resolves_of_one_name_share_a_single_doh_request():
    async def scenario():
        proxy=DirectProxy('127.0.0.1',port=0)
        async def get(*args,**kwargs):
            await asyncio.sleep(0.05)
            return httpx.Response(200,json={'Answer':[{'type':1,'data':'127.0.0.1'}]},request=httpx.Request('GET','https://dns.google/resolve'))
        proxy._http.get=AsyncMock(side_effect=get)
        assert await asyncio.gather(*(proxy.resolve('fixture.test') for _ in range(30)))==['127.0.0.1']*30
        assert proxy._http.get.await_count==1 and not proxy._resolving
        await proxy.stop()
    asyncio.run(scenario())


def test_cold_browser_burst_of_tunnels_fits_under_launchd_descriptor_limit():
    # launchd gives agents a soft limit of 256; scale it down: 40 loopback tunnels hold ~160 descriptors.
    code='''
import asyncio, resource
from unittest.mock import AsyncMock
from avito_bot.direct_proxy import DirectProxy
from avito_bot.runtime import raise_open_files_limit
resource.setrlimit(resource.RLIMIT_NOFILE,(64,resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
raise_open_files_limit()
async def main():
    async def echo(reader,writer):
        writer.write(await reader.read(5));await writer.drain();await reader.read();writer.close()
    upstream=await asyncio.start_server(echo,'127.0.0.1',0)
    proxy=DirectProxy('127.0.0.1',port=0);proxy.resolve=AsyncMock(return_value='127.0.0.1')
    await proxy.start()
    port=proxy._server.sockets[0].getsockname()[1];target=upstream.sockets[0].getsockname()[1]
    async def tunnel():
        reader,writer=await asyncio.open_connection('127.0.0.1',port)
        writer.write(b'CONNECT fixture.test:%d HTTP/1.1\\r\\n\\r\\n'%target)
        assert (await reader.readuntil(b'\\r\\n\\r\\n')).startswith(b'HTTP/1.1 200')
        writer.write(b'hello');assert await reader.readexactly(5)==b'hello'
        return writer
    writers=await asyncio.gather(*(tunnel() for _ in range(40)))
    for writer in writers:
        writer.close()
    proxy._server.close();upstream.close()
    await proxy._http.aclose()
    print('ok')
asyncio.run(main())
'''
    result=subprocess.run([sys.executable,'-c',code],capture_output=True,timeout=30)
    assert result.returncode==0 and result.stdout.strip()==b'ok', result.stderr.decode()[-2000:]


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


def test_source_startup_failure_keeps_telegram_and_worker_independent_and_closes_all(tmp_path, monkeypatch, caplog):
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
        assert self.source_down and 'RESTARTING' in self.status  # retried automatically, not a stop
        assert 'telegram started' in events and 'delivery started' in events
    monkeypatch.setattr(avito_bot.Monitor,'run',run_monitor)
    with caplog.at_level('INFO',logger='avito_bot'):
        asyncio.run(avito_bot.run())
    assert 'browser closed' in events and 'telegram closed' in events and 'delivery closed' in events
    instances[0].bot.session.close.assert_awaited_once()
    import os
    assert any('Application ready:' in r.message and f'pid={os.getpid()}' in r.message for r in caplog.records)


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
    loaded=SimpleNamespace(returncode=0,stdout='\tstate = running\n\tpid = 123\n',stderr='')
    missing=SimpleNamespace(returncode=113,stdout='',stderr='Could not find service "test-job" in domain for user gui: 501')
    results=iter([loaded,loaded,missing])
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


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    """All external service/process commands are mocked; data and locks are real."""
    import json
    import sqlite3
    from avito_bot import service
    root=tmp_path/'repo';data=root/'data';data.mkdir(parents=True)
    with sqlite3.connect(data/'bot.sqlite3') as conn:
        conn.execute('CREATE TABLE user_data(value TEXT)')
        conn.execute("INSERT INTO user_data VALUES('preserve outbox and subscriptions')")
    (root/'.env').write_text('BOT_TOKEN=private-fixture\n')
    profile=data/'browser-profile-chrome';profile.mkdir()
    (profile/'Cookies').write_bytes(b'private-cookie-fixture')
    (data/'deployment.json').write_text('{"status":"ready","revision":"old"}\n')
    (data/'bot.log').write_text('2026-10-24 20:00:00 INFO avito_bot: Application ready: '
                              'telegram=True delivery=True source=old pid=222\n')
    state=SimpleNamespace(root=root,data=data,plist=tmp_path/'LaunchAgents'/'bot.plist',
                          calls=[],loaded=True,ready=True,alive={111,222},
                          bootout_error=None,print_error=None,bootstrap_error=None,
                          parent=111,process_state='S',hook=None)
    def response(code=0,stdout='',stderr=''):
        return SimpleNamespace(returncode=code,stdout=stdout,stderr=stderr)
    def run(command,**kwargs):
        assert kwargs.get('timeout',0)>0, 'Every subprocess must have a timeout'
        state.calls.append(command)
        if command[0]=='git':return response(stdout='new-revision\n')
        if command[0]=='/bin/ps':return response(stdout=f'{state.parent} {state.process_state}\n')
        assert command[0]=='launchctl'
        assert kwargs['env']['LC_ALL']=='C'
        action=command[1]
        if state.hook:state.hook(action)
        if action=='bootout':
            if state.bootout_error:return state.bootout_error
            if not state.loaded:return response(3,stderr='Boot-out failed: 3: No such process')
            state.loaded=False
            return response()
        if action=='print':
            if state.print_error:return state.print_error
            if not state.loaded:return response(113,stderr=f'Could not find service "{service.LABEL}" in domain for user gui: 501')
            return response(stdout='\tstate = running\n\tpid = 111\n\tlast exit code = (never exited)\n'
                                    '\tresource coalition = {\n\t\tstate = active\n\t}\n')
        if action=='bootstrap':
            if state.bootstrap_error:return state.bootstrap_error
            state.loaded=True
            if state.ready:
                with (data/'bot.log').open('a') as log:
                    log.write('2026-10-25 20:00:00 INFO avito_bot: Application ready: '
                              'telegram=True delivery=True source=WAITING_FOR_USER pid=222\n')
            return response()
        raise AssertionError(command)
    def kill(pid,sig):
        assert sig==0, 'The installer must never signal/kill application processes'
        if pid not in state.alive:raise ProcessLookupError()
    monkeypatch.setattr(service.subprocess,'run',run)
    monkeypatch.setattr(service.os,'kill',kill)
    state.response=response
    state.install=lambda **kwargs:service.install_service(root,'/fixture/python','gui/501',state.plist,
                                                          startup_timeout=.03,**kwargs)
    state.old_manifest=(data/'deployment.json').read_bytes()
    return state


def assert_deployment_preserved(deployment):
    import sqlite3
    from avito_bot import service
    d=deployment
    assert (d.root/'.env').read_text()=='BOT_TOKEN=private-fixture\n'
    assert (d.data/'browser-profile-chrome'/'Cookies').read_bytes()==b'private-cookie-fixture'
    with sqlite3.connect(d.data/'bot.sqlite3') as conn:
        assert conn.execute('SELECT value FROM user_data').fetchone()[0]=='preserve outbox and subscriptions'
        assert conn.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    for path in (d.data/'deployment.lock',d.data/'bot.lock'):
        with service.installation_lock(path,timeout=0):pass


def test_successful_reinstallation_checks_child_and_waiting_for_user_is_ready(deployment):
    import json,sqlite3
    d=deployment
    result=d.install()
    assert result['status']=='ready' and result['service_pid']==111 and result['application_pid']==222
    assert json.loads((d.data/'deployment.json').read_text())['status']=='ready'
    assert json.loads((d.data/'deployment-attempt.json').read_text())['status']=='ready'
    with sqlite3.connect(result['backup']) as conn:
        assert conn.execute('SELECT value FROM user_data').fetchone()[0]=='preserve outbox and subscriptions'
    actions=[c[1] for c in d.calls if c[0]=='launchctl']
    assert actions==['bootout','print','bootstrap','print','print']
    assert d.calls.index(['launchctl','print','gui/501/local.avito-monitor']) < next(
        i for i,c in enumerate(d.calls) if c[0]=='git')
    assert_deployment_preserved(d)


def test_reinstallation_when_old_job_absent(deployment):
    deployment.loaded=False
    assert deployment.install()['status']=='ready'
    assert_deployment_preserved(deployment)


@pytest.mark.parametrize('phase',['bootstrap','manifest'])
def test_two_installers_cannot_overlap_even_after_bot_lock_is_released(deployment,monkeypatch,phase):
    import threading
    from avito_bot import service
    d=deployment
    entered=threading.Event();release=threading.Event();results=[]
    def pause():
        entered.set()
        assert release.wait(3)
    if phase=='bootstrap':
        d.hook=lambda action:pause() if action=='bootstrap' else None
    else:
        original=service.atomic_json
        def write(path,value):
            if path.name=='deployment.json':pause()
            original(path,value)
        monkeypatch.setattr(service,'atomic_json',write)
    def first():
        try:results.append(d.install())
        except BaseException as exc:results.append(exc)
    thread=threading.Thread(target=first)
    thread.start()
    try:
        assert entered.wait(3)
        with service.installation_lock(d.data/'bot.lock',timeout=0):pass
        before=list(d.calls)
        with pytest.raises(service.DeploymentError,match='deployment.lock'):
            d.install(lock_timeout=0)
        assert d.calls==before  # Losing installer did not even invoke bootout.
    finally:
        release.set();thread.join(3)
    assert not thread.is_alive() and len(results)==1
    assert not isinstance(results[0],BaseException),results
    assert results[0]['status']=='ready'
    assert_deployment_preserved(d)


def test_deployment_lock_timeout_does_not_touch_running_service(deployment):
    from avito_bot import service
    d=deployment
    with service.installation_lock(d.data/'deployment.lock',timeout=0):
        with pytest.raises(service.DeploymentError,match='deployment.lock'):
            d.install(lock_timeout=.01)
        assert d.calls==[] and d.loaded
        assert (d.data/'deployment.json').read_bytes()==d.old_manifest
        assert not (d.data/'deployment-attempt.json').exists()
    assert_deployment_preserved(d)


@pytest.mark.parametrize(('action','code','message'),[
    ('bootout',5,'Input/output error'),('bootout',1,'Operation not permitted'),
    ('print',5,'Input/output error'),('print',113,'Could not find domain for user gui: 501'),
    ('print',1,'Operation not permitted'),
    ('print',113,'Could not find service "a-different-job"'),
])
def test_unexpected_launchd_errors_abort_without_false_success(deployment,action,code,message):
    import json
    from avito_bot import service
    d=deployment
    setattr(d,action+'_error',d.response(code,stderr=message))
    with pytest.raises(service.DeploymentError,match=action+' failed'):
        d.install()
    assert not any(c[1]=='bootstrap' for c in d.calls if c[0]=='launchctl')
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert json.loads((d.data/'deployment-attempt.json').read_text())['status']=='failed'
    assert_deployment_preserved(d)


def test_missing_bootout_must_be_confirmed_by_print(deployment):
    from avito_bot import service
    d=deployment
    d.bootout_error=d.response(3,stderr='Boot-out failed: 3: No such process')
    with pytest.raises(service.DeploymentError,match='bootout failed'):
        d.install()  # print still sees a registered job: no blind fallback.
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert_deployment_preserved(d)


@pytest.mark.parametrize('action',['bootout','print','bootstrap'])
def test_subprocess_timeouts_abort_and_release_locks(deployment,action):
    import json
    from avito_bot import service
    d=deployment
    def timeout(current):
        if current==action:raise subprocess.TimeoutExpired(['launchctl',action],5)
    d.hook=timeout
    with pytest.raises(service.DeploymentError,match=action+' timed out'):
        d.install()
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert json.loads((d.data/'deployment-attempt.json').read_text())['status']=='failed'
    assert_deployment_preserved(d)


def test_bootstrap_failure_preserves_manifest_database_and_old_backups(deployment):
    from avito_bot import service
    d=deployment
    backups=d.data/'backups';backups.mkdir()
    old=backups/'older.sqlite3';old.write_bytes(b'existing backup stays unchanged')
    d.bootstrap_error=d.response(5,stderr='Input/output error')
    with pytest.raises(service.DeploymentError,match='bootstrap failed'):
        d.install()
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert old.read_bytes()==b'existing backup stays unchanged'
    assert len(list(backups.iterdir()))==2
    assert_deployment_preserved(d)


def test_bootstrap_with_no_running_process_is_failed(deployment):
    import json
    from avito_bot import service
    d=deployment;d.alive.clear()
    with pytest.raises(service.DeploymentError,match='startup failed'):
        d.install()
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert json.loads((d.data/'deployment-attempt.json').read_text())['status']=='failed'
    assert_deployment_preserved(d)


def test_bootstrap_running_without_fresh_readiness_is_only_registered(deployment):
    import json
    d=deployment;d.ready=False
    result=d.install()
    assert result['status']=='registered' and 'application_pid' not in result
    assert json.loads((d.data/'deployment.json').read_text())['status']=='registered'
    assert_deployment_preserved(d)


@pytest.mark.parametrize(('parent','state','alive'),[(999,'S',{111,222}),(111,'Z',{111,222}),(111,'S',{111})])
def test_stale_unrelated_or_dead_application_cannot_confirm_ready(deployment,parent,state,alive):
    d=deployment;d.parent=parent;d.process_state=state;d.alive=alive
    assert d.install()['status']=='registered'
    assert_deployment_preserved(d)


def test_bootstrap_error_preserves_manifest_even_if_registration_happened(deployment):
    from avito_bot import service
    d=deployment
    def bootstrap_failure(action):
        if action=='bootstrap':
            d.loaded=True
            raise subprocess.TimeoutExpired(['launchctl','bootstrap'],5)
    d.hook=bootstrap_failure
    with pytest.raises(service.DeploymentError,match='bootstrap timed out'):
        d.install()
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert_deployment_preserved(d)


def test_deadline_bounds_individual_print_timeout(monkeypatch):
    from avito_bot import service
    values=iter([100,100,124,126])
    monkeypatch.setattr(service.time,'monotonic',lambda:next(values))
    calls=[]
    def info(target,timeout):
        calls.append(timeout)
        return {'state':'running','pid':111}
    monkeypatch.setattr(service,'job_info',info)
    monkeypatch.setattr(service.time,'sleep',lambda _:None)
    with pytest.raises(service.DeploymentError,match='still unloading'):
        service.wait_for_unload('gui/501/test-job',timeout=25)
    assert calls==[5]


def test_stop_and_unload_share_one_deadline(monkeypatch):
    from avito_bot import service
    clock=[100.0];calls=[]
    monkeypatch.setattr(service.time,'monotonic',lambda:clock[0])
    def ctl(action,*args,timeout):
        calls.append((action,timeout))
        if action=='bootout':
            clock[0]=124
            return SimpleNamespace(returncode=0,stdout='',stderr='')
        return SimpleNamespace(returncode=113,stdout='',stderr='Could not find service "test-job"')
    monkeypatch.setattr(service,'launchctl',ctl)
    service.stop_service('gui/501/test-job',timeout=25)
    assert calls==[('bootout',25),('print',1)]


def test_launchctl_execution_error_is_not_treated_as_absence(deployment,monkeypatch):
    from avito_bot import service
    d=deployment
    monkeypatch.setattr(service.subprocess,'run',lambda *args,**kwargs:(_ for _ in ()).throw(
        FileNotFoundError(2,'No such file or directory')))
    with pytest.raises(service.DeploymentError,match='Cannot execute launchctl bootout'):
        d.install()
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert_deployment_preserved(d)


def test_app_exit_or_job_disappearance_after_ready_marker_is_not_success(deployment):
    from avito_bot import service
    d=deployment;prints=[]
    def disappear(action):
        if action=='print' and d.loaded:
            prints.append(action)
            if len(prints)>=2:d.loaded=False
    d.hook=disappear
    with pytest.raises(service.DeploymentError,match='startup failed'):
        d.install()
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert_deployment_preserved(d)


def test_new_log_after_rotation_can_confirm_readiness(deployment):
    d=deployment
    def rotate(action):
        if action=='bootstrap':
            (d.data/'bot.log').rename(d.data/'bot.log.1')
            (d.data/'bot.log').touch()
    d.hook=rotate
    assert d.install()['status']=='ready'
    assert_deployment_preserved(d)


def test_backup_error_stops_before_bootstrap_and_releases_both_locks(deployment,monkeypatch):
    from avito_bot import service
    d=deployment
    monkeypatch.setattr(service,'backup_database',lambda *args:(_ for _ in ()).throw(OSError('backup failed')))
    with pytest.raises(service.DeploymentError,match='backup failed'):
        d.install()
    assert not any(c[1]=='bootstrap' for c in d.calls if c[0]=='launchctl')
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert_deployment_preserved(d)


def test_atomic_manifest_failure_keeps_previous_success_and_removes_partial_file(deployment,monkeypatch):
    from pathlib import Path
    from avito_bot import service
    d=deployment
    original=Path.replace
    def fail_manifest(path,target):
        if path.name=='deployment.json.tmp':raise OSError('manifest replace failed')
        return original(path,target)
    monkeypatch.setattr(Path,'replace',fail_manifest)
    with pytest.raises(service.DeploymentError,match='manifest replace failed'):
        d.install()
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert not (d.data/'deployment.json.tmp').exists()
    assert_deployment_preserved(d)


def test_dry_run_without_telegram_is_not_reported_as_ready(deployment):
    d=deployment
    def dry_run(action):
        if action=='bootstrap':
            d.ready=False
            with (d.data/'bot.log').open('a') as log:
                log.write('2026-10-25 20:00:00 INFO avito_bot: Application ready: '
                          'telegram=False delivery=False source=ACTIVE pid=222\n')
    d.hook=dry_run
    assert d.install()['status']=='registered'
    assert_deployment_preserved(d)


def test_busy_application_lock_prevents_bootstrap_even_with_deployment_lock(deployment,monkeypatch):
    from avito_bot import service
    d=deployment
    original=service.installation_lock
    def bounded(path,timeout=25):
        return original(path,timeout=0 if path.name=='bot.lock' else timeout)
    with original(d.data/'bot.lock',timeout=0):
        monkeypatch.setattr(service,'installation_lock',bounded)
        with pytest.raises(service.DeploymentError,match='bot.lock'):
            d.install()
        assert not any(c[1]=='bootstrap' for c in d.calls if c[0]=='launchctl')
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert_deployment_preserved(d)


def test_separate_installer_process_cannot_stop_service_while_lock_is_held(deployment):
    from avito_bot import service
    d=deployment
    code='''
from pathlib import Path
from avito_bot import service
import sys

def forbidden(*args, **kwargs):
    raise AssertionError('Contender must never call launchctl')
service.launchctl=forbidden
try:
    service.install_service(Path(sys.argv[1]),sys.executable,'gui/501',Path(sys.argv[2]),lock_timeout=0)
except service.DeploymentError as exc:
    assert 'deployment.lock' in str(exc)
    print('deployment blocked before launchctl')
else:
    raise AssertionError('Concurrent installation was allowed')
'''
    with service.installation_lock(d.data/'deployment.lock',timeout=0):
        child=subprocess.Popen([sys.executable,'-c',code,str(d.root),str(d.plist)],
                               stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            stdout,stderr=child.communicate(timeout=10)
        finally:
            if child.poll() is None:
                child.kill();child.wait(timeout=5)
        assert child.returncode==0,stderr
        assert stdout.strip()=='deployment blocked before launchctl'
        assert (d.data/'deployment.json').read_bytes()==d.old_manifest
        assert d.calls==[]
    assert_deployment_preserved(d)


def test_invalid_sqlite_backup_reports_failure_without_overwriting_source(deployment):
    import sqlite3
    from avito_bot import service
    d=deployment
    (d.data/'bot.sqlite3').write_bytes(b'original damaged database bytes')
    with pytest.raises(service.DeploymentError,match='not a database'):
        d.install()
    assert (d.data/'bot.sqlite3').read_bytes()==b'original damaged database bytes'
    assert (d.data/'deployment.json').read_bytes()==d.old_manifest
    assert not any(c[1]=='bootstrap' for c in d.calls if c[0]=='launchctl')
    for path in (d.data/'deployment.lock',d.data/'bot.lock'):
        with service.installation_lock(path,timeout=0):pass


@pytest.mark.parametrize(('status','exit_code','message'),[('ready',0,'Ready'),('registered',2,'Registered')])
def test_install_cli_distinguishes_ready_and_registered(deployment,monkeypatch,capsys,status,exit_code,message):
    from avito_bot import service
    d=deployment
    monkeypatch.setattr(service.sys,'platform','darwin')
    monkeypatch.setattr(service.sys,'argv',['avito-service','install'])
    monkeypatch.setattr(service.os,'getuid',lambda:501,raising=False)
    monkeypatch.setattr(service,'install_service',lambda *args,**kwargs:{'status':status,'revision':'new','backup':None})
    if exit_code:
        with pytest.raises(SystemExit) as exc:service.main()
        assert exc.value.code==exit_code
    else:
        service.main()
    output=capsys.readouterr()
    assert output.out.startswith(message)
    if status=='registered':assert 'Ready' not in output.out
    assert d.calls==[]


def test_install_cli_reports_failure_without_success(deployment,monkeypatch,capsys):
    from avito_bot import service
    monkeypatch.setattr(service.sys,'platform','darwin')
    monkeypatch.setattr(service.sys,'argv',['avito-service','install'])
    monkeypatch.setattr(service.os,'getuid',lambda:501,raising=False)
    monkeypatch.setattr(service,'install_service',lambda *args,**kwargs:(_ for _ in ()).throw(
        service.DeploymentError('launchd unavailable')))
    with pytest.raises(SystemExit) as exc:service.main()
    assert exc.value.code==1
    output=capsys.readouterr()
    assert 'Deployment failed: launchd unavailable' in output.err
    assert not output.out
