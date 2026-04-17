@echo off
setlocal
cd /d "%~dp0.."
cargo run --release -- init
exit /b %ERRORLEVEL%
