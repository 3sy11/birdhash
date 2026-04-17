@echo off
setlocal
cd /d "%~dp0.."
cargo run --release -- collide --threads 8
exit /b %ERRORLEVEL%
