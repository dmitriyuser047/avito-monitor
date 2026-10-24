"""Install the local macOS LaunchAgent without embedding credentials in its plist."""
import argparse
import json
import os
from pathlib import Path
import plistlib
import re
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime

from .config import ROOT
from .runtime import InstanceLock

LABEL = 'local.avito-monitor'


@contextmanager
def installation_lock(path: Path, timeout: float = 25):
    """launchctl bootout returns before the application's cleanup always ends."""
    deadline = time.monotonic() + timeout
    while True:
        lock = InstanceLock(path)
        try:
            lock.__enter__()
            break
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise RuntimeError('Previous instance still holds the lock; deployment was not started') from None
            time.sleep(.2)
    try:
        yield
    finally:
        lock.__exit__(None,None,None)


def backup_database(path: Path, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    target = directory / f'bot-{datetime.now():%Y%m%d-%H%M%S-%f}.sqlite3'
    with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as src, sqlite3.connect(target) as dst:
        src.backup(dst)
        if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise RuntimeError('Backup integrity check failed')
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


def main():
    parser = argparse.ArgumentParser(description='Local macOS service deployment')
    parser.add_argument('action', choices=['install', 'stop', 'status'])
    args = parser.parse_args()
    if sys.platform != 'darwin':
        parser.error('This command installs a macOS LaunchAgent. Other systems use start.sh/start.bat.')
    domain = f'gui/{os.getuid()}'
    target = f'{domain}/{LABEL}'
    if args.action == 'status':
        result = subprocess.run(['launchctl','print',target],capture_output=True,text=True)
        if result.returncode:
            print('Service is not loaded')
            raise SystemExit(1)
        for line in result.stdout.splitlines():
            if re.match(r'\s*(state|pid|last exit code|runs) =',line):
                print(line.strip())
        return
    if args.action == 'stop':
        result = subprocess.run(['launchctl','bootout',target],capture_output=True,text=True)
        print('Service stopped' if not result.returncode else 'Service is not loaded')
        return
    # Re-deployment first stops only our registered service. A manually started bot
    # is protected by the OS lock and must not be killed by a broad process match.
    subprocess.run(['launchctl','bootout',target],capture_output=True)
    data = ROOT/'data'
    data.mkdir(mode=0o700,exist_ok=True)
    with installation_lock(data/'bot.lock'):
        backup = backup_database(data/'bot.sqlite3',data/'backups') if (data/'bot.sqlite3').exists() else None
        plist_path = Path.home()/'Library'/'LaunchAgents'/f'{LABEL}.plist'
        plist_path.parent.mkdir(parents=True,exist_ok=True)
        plist_path.write_bytes(plistlib.dumps(launch_agent(ROOT,sys.executable)))
        plist_path.chmod(0o600)
        revision = subprocess.run(['git','rev-parse','HEAD'],cwd=ROOT,capture_output=True,text=True,check=True).stdout.strip()
        manifest = {'revision':revision, 'backup':str(backup) if backup else None,
                    'deployed_at':datetime.now().astimezone().isoformat()}
        (data/'deployment.json').write_text(json.dumps(manifest,indent=2)+'\n')
    subprocess.run(['launchctl','bootstrap',domain,str(plist_path)],check=True)
    print(f'Deployed {revision[:12]}; backup: {backup}; service: {LABEL}')
