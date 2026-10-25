#!/usr/bin/env bash
# Авито-монитор: установка и запуск на macOS / Linux.
# Первый запуск ставит всё нужное и спрашивает токен бота и город; дальше — просто запускает.
# Бот перезапускается сам, если упадёт. Остановить — Ctrl+C или закрыть окно.
set -uo pipefail
cd "$(dirname "$0")"

say() { printf '\n== %s\n' "$1"; }

# macOS: не давать компьютеру уснуть, пока работает скрипт (блокировка экрана не мешает;
# закрытая крышка — усыпит, если не подключён внешний монитор)
if [ "$(uname)" = "Darwin" ]; then
    caffeinate -is -w $$ &
fi

# 1. uv — ставит Python нужной версии и все библиотеки
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
if ! command -v uv >/dev/null 2>&1; then
    say "Устанавливаю uv (менеджер Python)"
    curl -LsSf https://astral.sh/uv/install.sh | sh || { echo "Не удалось установить uv"; exit 1; }
    export PATH="$HOME/.local/bin:$PATH"
fi

# 2. Библиотеки и браузер (повторный запуск быстрый — ставится только недостающее)
say "Проверяю зависимости"
uv sync --frozen --no-dev --inexact || { echo "Не удалось установить зависимости"; exit 1; }
uv run playwright install chromium || { echo "Не удалось установить браузер Chromium"; exit 1; }

# 3. Настройка при первом запуске
uv run avito-setup --if-needed || exit 1

# 4. Один общий контроллер с ограниченным числом перезапусков.
say "Бот запускается. Ctrl+C — остановить."
exec uv run --no-dev avito-supervise
