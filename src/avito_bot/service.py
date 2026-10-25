"""Bounded, serialized installation of the local macOS LaunchAgent."""
import argparse
from contextlib import closing, contextmanager
from datetime import datetime
import json
import os
from pathlib import Path
import plistlib
import re
import sqlite3
import subprocess
import sys
import time

from .config import ROOT
from .runtime import InstanceLock

LABEL = 'local.avito-monitor'
COMMAND_TIMEOUT = 5
UNLOAD_TIMEOUT = 25
START_TIMEOUT = 30


class DeploymentError(RuntimeError):
    pass


def launchctl(*args: str, timeout: float = COMMAND_TIMEOUT):
    try:
        return subprocess.run(['launchctl', *args], capture_output=True, text=True,
                              timeout=timeout, env={**os.environ, 'LC_ALL': 'C', 'LANG': 'C'})
    except subprocess.TimeoutExpired:
        raise DeploymentError(f'launchctl {args[0]} timed out after {timeout:g}s') from None
    except OSError as exc:
        raise DeploymentError(f'Cannot execute launchctl {args[0]}: {exc.strerror}') from None


def command_error(action, result):
    detail = (result.stderr or '').strip()[:400]
    return DeploymentError(f'launchctl {action} failed ({result.returncode}): {detail or "no diagnostic"}')


def job_info(target: str, timeout: float = COMMAND_TIMEOUT) -> dict | None:
    result = launchctl('print', target, timeout=timeout)
    if result.returncode:
        label = target.rsplit('/', 1)[-1]
        missing = f'Could not find service "{label}"' in result.stderr
        if result.returncode in (3, 113) and missing:
            return None
        raise command_error('print', result)
    # print is diagnostic text, not a stable API. Missing/changed fields can never
    # prove readiness: the registration then remains explicitly unconfirmed.
    def field(name):
        matches = re.findall(rf'^([ \t]+){re.escape(name)} = (.+)$', result.stdout, re.M)
        return min(matches, key=lambda m: len(m[0].expandtabs()))[1].strip() if matches else None
    pid = field('pid')
    return {'state': field('state'), 'pid': int(pid) if pid and pid.isdigit() else None,
            'last_exit_code': field('last exit code')}


def wait_for_unload(target: str, timeout: float = UNLOAD_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeploymentError('Previous LaunchAgent is still unloading; deployment was not started')
        if job_info(target, timeout=min(COMMAND_TIMEOUT, remaining)) is None:
            return
        time.sleep(min(.2, max(0, deadline-time.monotonic())))


def stop_service(target: str, timeout: float = UNLOAD_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    result = launchctl('bootout', target, timeout=timeout)
    if result.returncode:
        # Only documented missing-service diagnostics are candidates; a second
        # print must confirm the *job*, rather than the GUI domain, is absent.
        missing = ((result.returncode == 3 and 'No such process' in result.stderr)
                   or (result.returncode == 113 and 'Could not find specified service' in result.stderr))
        if not missing:
            raise command_error('bootout', result)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise DeploymentError('Timed out stopping the previous LaunchAgent')
    if result.returncode:
        if job_info(target, timeout=min(COMMAND_TIMEOUT, remaining)) is not None:
            raise command_error('bootout', result)
    else:
        wait_for_unload(target, timeout=remaining)


@contextmanager
def installation_lock(path: Path, timeout: float = 25):
    deadline = time.monotonic() + timeout
    while True:
        lock = InstanceLock(path)
        try:
            lock.__enter__()
            break
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise DeploymentError(f'{path.name}: previous instance still holds the lock; deployment was not started') from None
            time.sleep(min(.2, max(0, deadline-time.monotonic())))
    try:
        yield
    finally:
        lock.__exit__(None, None, None)


def backup_database(path: Path, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    target = directory / f'bot-{datetime.now():%Y%m%d-%H%M%S-%f}.sqlite3'
    with closing(sqlite3.connect(f'file:{path}?mode=ro', uri=True)) as src, closing(sqlite3.connect(target)) as dst:
        src.backup(dst)
        if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise DeploymentError('Backup integrity check failed')
    target.chmod(0o600)
    return target


def launch_agent(root: Path, executable: str) -> dict:
    return {'Label': LABEL,
            'ProgramArguments': [executable, '-c', 'from avito_bot.supervisor import main; main()'],
            'WorkingDirectory': str(root), 'RunAtLoad': True, 'KeepAlive': False,
            'EnvironmentVariables': {'AVITO_SERVICE': '1', 'PYTHONUNBUFFERED': '1'},
            'StandardOutPath': str(root/'data'/'service.log'),
            'StandardErrorPath': str(root/'data'/'service.log'),
            'ProcessType': 'Interactive', 'ExitTimeOut': 20}


def atomic_json(path: Path, value: dict) -> None:
    pending = path.with_name(path.name+'.tmp')
    try:
        pending.write_text(json.dumps(value, indent=2)+'\n', encoding='utf-8')
        pending.chmod(0o600)
        pending.replace(path)
    finally:
        pending.unlink(missing_ok=True)


def log_checkpoint(path: Path):
    try:
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size
    except FileNotFoundError:
        return None


def ready_pids(path: Path, checkpoint) -> set[int]:
    try:
        with path.open('rb') as log:
            stat = os.fstat(log.fileno())
            if checkpoint and (stat.st_dev, stat.st_ino) == checkpoint[:2] and stat.st_size >= checkpoint[2]:
                log.seek(checkpoint[2])
            text = log.read(1_000_000).decode('utf-8', errors='replace')
    except FileNotFoundError:
        return set()
    return {int(pid) for pid in re.findall(
        r'^.* INFO avito_bot: Application ready: telegram=True delivery=True .*\bpid=(\d+)\s*$', text, re.M)}


def process_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def application_running(app_pid: int, service_pid: int, timeout: float) -> bool:
    if not process_alive(app_pid):
        return False
    try:
        result = subprocess.run(['/bin/ps', '-o', 'ppid=,stat=', '-p', str(app_pid)],
                                capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return False
    fields = result.stdout.split()
    return (result.returncode == 0 and len(fields) == 2 and fields[0] == str(service_pid)
            and not any(flag in fields[1] for flag in 'ZXT'))


def verify_start(target: str, log_path: Path, checkpoint, timeout: float = START_TIMEOUT) -> dict:
    deadline = time.monotonic() + timeout
    info = None
    while True:
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            break
        info = job_info(target, timeout=min(COMMAND_TIMEOUT, remaining))
        pid = info['pid'] if info else None
        if info and info['state'] == 'running' and process_alive(pid):
            for app_pid in sorted(ready_pids(log_path, checkpoint), reverse=True):
                remaining = deadline-time.monotonic()
                if remaining > 0 and application_running(app_pid, pid, min(COMMAND_TIMEOUT, remaining)):
                    # Re-check launchd after the child diagnostic to exclude an
                    # exited/replaced supervisor during this verification.
                    remaining = deadline-time.monotonic()
                    if remaining <= 0:
                        break
                    confirmed = job_info(target, timeout=min(COMMAND_TIMEOUT, remaining))
                    info = confirmed
                    if confirmed and confirmed['state'] == 'running' and confirmed['pid'] == pid and process_alive(pid):
                        return {'status': 'ready', 'service_pid': pid, 'application_pid': app_pid}
        time.sleep(min(.2, max(0, deadline-time.monotonic())))
    if info and info['state'] == 'running' and process_alive(info['pid']):
        return {'status': 'registered', 'service_pid': info['pid'],
                'reason': 'Process is running, but fresh application readiness was not confirmed in time'}
    raise DeploymentError('LaunchAgent startup failed: no registered running process before the deadline')


def install_service(root: Path, executable: str, domain: str, plist_path: Path,
                    lock_timeout: float = 25, startup_timeout: float = START_TIMEOUT) -> dict:
    data = root/'data'
    data.mkdir(mode=0o700, exist_ok=True)
    target = f'{domain}/{LABEL}'
    # This lock covers *all* launchd mutations, including bootstrap and manifest.
    # Losing contenders must not stop the successful installer's running process.
    with installation_lock(data/'deployment.lock', timeout=lock_timeout):
        attempt = {'status': 'failed', 'attempted_at': datetime.now().astimezone().isoformat()}
        try:
            stop_service(target)
            with installation_lock(data/'bot.lock'):
                backup = backup_database(data/'bot.sqlite3', data/'backups') if (data/'bot.sqlite3').exists() else None
                plist_path.parent.mkdir(parents=True, exist_ok=True)
                pending = plist_path.with_name(plist_path.name+'.tmp')
                try:
                    pending.write_bytes(plistlib.dumps(launch_agent(root, executable)))
                    pending.chmod(0o600)
                    pending.replace(plist_path)
                finally:
                    pending.unlink(missing_ok=True)
                revision = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, capture_output=True,
                                          text=True, check=True, timeout=COMMAND_TIMEOUT).stdout.strip()
                attempt.update(revision=revision, backup=str(backup) if backup else None)
                checkpoint = log_checkpoint(data/'bot.log')
            # bot.lock must be free for the application child; deployment.lock
            # remains held while it starts and the manifest is committed.
            result = launchctl('bootstrap', domain, str(plist_path))
            if result.returncode:
                raise command_error('bootstrap', result)
            attempt.update(verify_start(target, data/'bot.log', checkpoint, timeout=startup_timeout))
            attempt['deployed_at'] = datetime.now().astimezone().isoformat()
            atomic_json(data/'deployment.json', attempt)
        except (DeploymentError, OSError, sqlite3.Error, subprocess.SubprocessError) as exc:
            attempt.update(status='failed', reason=str(exc))
            atomic_json(data/'deployment-attempt.json', attempt)
            raise DeploymentError(str(exc)) from None
        atomic_json(data/'deployment-attempt.json', attempt)
        return attempt


def main():
    parser = argparse.ArgumentParser(description='Local macOS service deployment')
    parser.add_argument('action', choices=['install', 'stop', 'status'])
    args = parser.parse_args()
    if sys.platform != 'darwin':
        parser.error('This command installs a macOS LaunchAgent. Other systems use start.sh/start.bat.')
    domain = f'gui/{os.getuid()}'
    target = f'{domain}/{LABEL}'
    try:
        if args.action == 'status':
            info = job_info(target)
            if info is None:
                print('Service is not loaded')
                raise SystemExit(1)
            print(f"state = {info['state']}\npid = {info['pid']}\nlast exit code = {info['last_exit_code']}")
        elif args.action == 'stop':
            (ROOT/'data').mkdir(mode=0o700, exist_ok=True)
            with installation_lock(ROOT/'data'/'deployment.lock'):
                stop_service(target)
            print('Service stopped')
        else:
            result = install_service(ROOT, sys.executable, domain,
                                     Path.home()/'Library'/'LaunchAgents'/f'{LABEL}.plist')
            if result['status'] == 'ready':
                print(f"Ready {result['revision'][:12]}; backup: {result['backup']}; service: {LABEL}")
            else:
                print(f"Registered {result['revision'][:12]}; application readiness not confirmed. See data/deployment.json")
                raise SystemExit(2)
    except (DeploymentError, OSError) as exc:
        parser.exit(1, f'Deployment failed: {exc}\n')
