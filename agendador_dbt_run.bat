@echo off
REM Ativa o ambiente virtual e roda o dbt

REM Caminho da pasta do projeto
cd C:\Git\sb_loja_virtual

REM Ativa o ambiente virtual do Python (ajuste se estiver diferente)
call "C:\Git\sb_loja_virtual\.venv\Scripts\activate.bat"

REM Executa o DBT
dbt run --exclude tag:snaps
IF %ERRORLEVEL% NEQ 0 GOTO erro

dbt snapshot
IF %ERRORLEVEL% NEQ 0 GOTO erro

dbt run --select tag:snaps
IF %ERRORLEVEL% NEQ 0 GOTO erro

REM (opcional) Salva o log num arquivo pra auditoria
REM dbt run --profiles-dir "C:\Users\HTK1511\.dbt" > dbt_log.txt 2>&1