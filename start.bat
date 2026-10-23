@echo off
rem Авито-монитор: установка и запуск на Windows. Запуск — двойной щелчок.
rem Первый запуск ставит всё нужное и спрашивает токен бота и город; дальше — просто запускает.
rem Бот перезапускается сам, если упадёт. Остановить — закрыть окно.
chcp 65001 >nul
setlocal
cd /d "%~dp0"
set "PYTHONIOENCODING=utf-8"
set "PATH=%USERPROFILE%\.local\bin;%PATH%"

where uv >nul 2>nul
if errorlevel 1 (
    echo == Устанавливаю uv ^(менеджер Python^)
    powershell -NoProfile -ExecutionPolicy Bypass -Command "irm https://astral.sh/uv/install.ps1 | iex"
    if errorlevel 1 goto fail
)

echo == Проверяю зависимости
uv sync --frozen --no-dev
if errorlevel 1 goto fail
uv run playwright install chromium
if errorlevel 1 goto fail

uv run avito-setup --if-needed
if errorlevel 1 goto fail

:loop
echo.
echo == Бот запущен %date% %time:~0,5%. Окно не закрывайте.
uv run avito-bot
echo Бот остановился. Перезапуск через 10 секунд...
timeout /t 10 /nobreak >nul
goto loop

:fail
echo.
echo Что-то пошло не так — сообщение об ошибке выше.
pause
exit /b 1
