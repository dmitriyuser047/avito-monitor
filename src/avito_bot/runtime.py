"""Cross-platform single instance lock. OS releases it even after a crash."""
import os
from pathlib import Path


class InstanceLock:
    def __init__(self, path: Path):
        self.path, self.file = path, None

    def __enter__(self):
        self.file = self.path.open('a+b')
        self.path.chmod(0o600)
        try:
            if os.name == 'nt':
                import msvcrt
                self.file.seek(0)
                self.file.write(b'0')
                self.file.flush()
                self.file.seek(0)
                try:
                    msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    raise BlockingIOError('Instance is already running') from exc
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.file.close()
            raise
        return self

    def __exit__(self, *_):
        self.file.close()
