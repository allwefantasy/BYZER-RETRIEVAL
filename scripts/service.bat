@echo off
setlocal enabledelayedexpansion

:: -----------------------------------------------------------------------------
:: 服务管理脚本 - Windows版本
:: -----------------------------------------------------------------------------

:: 解析脚本目录
set "SCRIPT_DIR=%~dp0"
set "BASE_DIR=%SCRIPT_DIR%.."

:: 设置环境
set "JAVA_HOME=%BASE_DIR%\jdk"
set "LIBS_DIR=%BASE_DIR%\libs"
set "LOGS_DIR=%BASE_DIR%\logs"
set "PID_FILE=%BASE_DIR%\service.pid"
set "LOG_FILE=%LOGS_DIR%\service.log"
set "ERROR_LOG=%LOGS_DIR%\error.log"

:: 根据操作系统检测Java命令路径
set "JAVA_CMD=%JAVA_HOME%\bin\java.exe"

:: 应用主类 - 更改为你的实际主类
set "MAIN_CLASS=tech.mlsql.retrieval.RetrievalFlightServer"

:: JVM选项 - 使用ZGC替代G1GC
set "JVM_OPTS=-Xms1g -Xmx4g -XX:+UseZGC -XX:+UnlockExperimentalVMOptions -XX:ConcGCThreads=2 -XX:ZCollectionInterval=120 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%LOGS_DIR% --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.annotation=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.lang.module=ALL-UNNAMED --add-opens java.base/java.lang.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util.concurrent.locks=ALL-UNNAMED --add-opens java.base/java.util.function=ALL-UNNAMED --add-opens java.base/java.util.jar=ALL-UNNAMED --add-opens java.base/java.util.regex=ALL-UNNAMED --add-opens java.base/java.util.stream=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED --add-opens java.base/java.util.spi=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --enable-preview --add-modules jdk.incubator.vector"

:: 应用选项
set "APP_OPTS="

:: 创建日志目录（如果不存在）
if not exist "%LOGS_DIR%" mkdir "%LOGS_DIR%"

:: -----------------------------------------------------------------------------
:: 功能函数
:: -----------------------------------------------------------------------------

:: 检查服务是否运行
:is_running
if not exist "%PID_FILE%" (
    exit /b 1
) else (
    set /p PID=<"%PID_FILE%"
    tasklist /FI "PID eq !PID!" 2>nul | find "!PID!" >nul
    if !ERRORLEVEL! == 0 (
        exit /b 0
    ) else (
        exit /b 1
    )
)

:: 构建包含libs目录中所有jar的类路径
:build_classpath
set "CLASSPATH="
for %%F in ("%LIBS_DIR%\*.jar") do (
    if "!CLASSPATH!" == "" (
        set "CLASSPATH=%%F"
    ) else (
        set "CLASSPATH=!CLASSPATH!;%%F"
    )
)
goto :eof

:: 启动服务
:start_service
call :is_running
if !ERRORLEVEL! == 0 (
    set /p PID=<"%PID_FILE%"
    echo 服务已在运行中，PID为 !PID!
    goto :eof
)

call :build_classpath

echo 正在启动服务...
echo 使用JAVA_HOME: %JAVA_HOME%
echo 使用类路径: %CLASSPATH%

:: 使用wmic创建进程并获取PID
wmic process call create "cmd.exe /c %JAVA_CMD% %JVM_OPTS% -cp %CLASSPATH% %MAIN_CLASS% %APP_OPTS% > %LOG_FILE% 2> %ERROR_LOG%" | find "ProcessId" > "%TEMP%\pid.txt"
for /f "tokens=2 delims==" %%A in ('type "%TEMP%\pid.txt"') do (
    for /f "tokens=1 delims= " %%B in ("%%A") do (
        set "PID=%%B"
    )
)
del "%TEMP%\pid.txt"

echo !PID! > "%PID_FILE%"

:: 检查进程是否成功启动
timeout /t 2 /nobreak > nul
tasklist /FI "PID eq !PID!" 2>nul | find "!PID!" >nul
if !ERRORLEVEL! == 0 (
    echo 服务成功启动，PID为 !PID!
) else (
    echo 服务启动失败，请查看日志 %ERROR_LOG%
    if exist "%PID_FILE%" del "%PID_FILE%"
    exit /b 1
)
goto :eof

:: 停止服务
:stop_service
call :is_running
if !ERRORLEVEL! == 1 (
    echo 服务未运行
    goto :eof
)

set /p PID=<"%PID_FILE%"
echo 正在停止PID为 !PID! 的服务...

taskkill /PID !PID!

:: 等待进程终止
set /a timeout=30
set /a count=0
:wait_loop
tasklist /FI "PID eq !PID!" 2>nul | find "!PID!" >nul
if !ERRORLEVEL! == 0 (
    if !count! geq !timeout! (
        echo 服务在 !timeout! 秒后未正常停止，强制终止...
        taskkill /F /PID !PID!
        timeout /t 2 /nobreak > nul
        goto :check_stopped
    )
    timeout /t 1 /nobreak > nul
    set /a count+=1
    goto :wait_loop
)

:check_stopped
tasklist /FI "PID eq !PID!" 2>nul | find "!PID!" >nul
if !ERRORLEVEL! == 1 (
    echo 服务已成功停止
    if exist "%PID_FILE%" del "%PID_FILE%"
) else (
    echo 停止服务失败
    exit /b 1
)
goto :eof

:: 重启服务
:restart_service
call :stop_service
timeout /t 2 /nobreak > nul
call :start_service
goto :eof

:: 显示服务状态
:show_status
call :is_running
if !ERRORLEVEL! == 0 (
    set /p PID=<"%PID_FILE%"
    echo 服务正在运行，PID为 !PID!
    echo 进程详情:
    tasklist /FI "PID eq !PID!" /V
) else (
    echo 服务未运行
    if exist "%PID_FILE%" (
        echo 存在过期的PID文件，正在清理...
        del "%PID_FILE%"
    )
)
goto :eof

:: 显示日志
:show_logs
if "%2"=="follow" (
    if exist "%LOG_FILE%" (
        powershell -command "Get-Content '%LOG_FILE%' -Tail 100 -Wait"
    ) else (
        echo 未找到日志文件: %LOG_FILE%
    )
) else (
    if exist "%LOG_FILE%" (
        powershell -command "Get-Content '%LOG_FILE%' -Tail 100"
    ) else (
        echo 未找到日志文件: %LOG_FILE%
    )
)
goto :eof

:: 显示错误日志
:show_error_logs
if "%2"=="follow" (
    if exist "%ERROR_LOG%" (
        powershell -command "Get-Content '%ERROR_LOG%' -Tail 100 -Wait"
    ) else (
        echo 未找到错误日志文件: %ERROR_LOG%
    )
) else (
    if exist "%ERROR_LOG%" (
        powershell -command "Get-Content '%ERROR_LOG%' -Tail 100"
    ) else (
        echo 未找到错误日志文件: %ERROR_LOG%
    )
)
goto :eof

:: 显示使用信息
:show_usage
echo 用法: %0 {start^|stop^|restart^|status^|logs^|errorlogs^|help}
echo.
echo 命令:
echo   start      启动服务
echo   stop       停止服务
echo   restart    重启服务
echo   status     检查服务状态
echo   logs       显示最近100行日志
echo   logs -f    持续跟踪日志
echo   errorlogs  显示最近100行错误日志
echo   errorlogs -f 持续跟踪错误日志
echo   help       显示此帮助信息
goto :eof

:: -----------------------------------------------------------------------------
:: 脚本主执行部分
:: -----------------------------------------------------------------------------

:: 验证JAVA_HOME
if not exist "%JAVA_CMD%" (
    echo 错误: JAVA_HOME设置不正确。无法在 %JAVA_CMD% 找到Java可执行文件
    exit /b 1
)

:: 检查libs目录是否存在并包含jar文件
set "JAR_COUNT=0"
if exist "%LIBS_DIR%\*.jar" (
    for %%F in ("%LIBS_DIR%\*.jar") do set /a JAR_COUNT+=1
)
if %JAR_COUNT% equ 0 (
    echo 错误: 在 %LIBS_DIR% 中未找到JAR文件
    exit /b 1
)

:: 处理命令行参数
if "%1"=="" (
    call :show_usage
    exit /b 1
)

if "%1"=="start" (
    call :start_service
) else if "%1"=="stop" (
    call :stop_service
) else if "%1"=="restart" (
    call :restart_service
) else if "%1"=="status" (
    call :show_status
) else if "%1"=="logs" (
    if "%2"=="-f" (
        call :show_logs "follow"
    ) else (
        call :show_logs
    )
) else if "%1"=="errorlogs" (
    if "%2"=="-f" (
        call :show_error_logs "follow"
    ) else (
        call :show_error_logs
    )
) else if "%1"=="help" (
    call :show_usage
) else if "%1"=="--help" (
    call :show_usage
) else if "%1"=="-h" (
    call :show_usage
) else (
    echo 未知命令: %1
    call :show_usage
    exit /b 1
)

exit /b 0 