"""Prepare a local unpacked extension; never copy cookies or modify Chrome's profile."""
import argparse
import json
import shutil
from pathlib import Path

from .config import load_config
from .extension_session import pairing_token


def prepare_extension(data_dir: Path, port=9233):
    from .scraper import INSPECT_FN, LIGHT_JS
    directory = data_dir / 'chrome-extension'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    token = pairing_token(data_dir / 'extension-bridge')
    for source in (Path(__file__).parent / 'chrome_extension').iterdir():
        if source.is_file():
            shutil.copyfile(source, directory / source.name)
    (directory / 'settings.js').write_text(
        f'export const ENDPOINT = {json.dumps(f"ws://127.0.0.1:{port}/bridge")};\n'
        f'export const TOKEN = {json.dumps(token)};\n')
    (directory / 'settings.js').chmod(0o600)
    (directory / 'inspector.js').write_text(
        f'export const inspectDocument = () => ({{...({INSPECT_FN})(document), document_started_at: performance.timeOrigin / 1000}});\n'
        f'export const fetchDocument = {LIGHT_JS};\n')
    return directory


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    cfg = load_config()
    print(prepare_extension(cfg.data_dir, cfg.extension_bridge_port))
