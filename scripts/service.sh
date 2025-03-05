#!/bin/bash

# -----------------------------------------------------------------------------
# 服务管理脚本
# -----------------------------------------------------------------------------

# 解析脚本目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

# 设置环境
JAVA_HOME="$BASE_DIR/jdk"
LIBS_DIR="$BASE_DIR/libs"
LOGS_DIR="$BASE_DIR/logs"
PID_FILE="$BASE_DIR/service.pid"
LOG_FILE="$LOGS_DIR/service.log"
ERROR_LOG="$LOGS_DIR/error.log"

# Java命令完整路径
JAVA_CMD="$JAVA_HOME/bin/java"

# 应用主类 - 更改为你的实际主类
MAIN_CLASS="tech.mlsql.retrieval.RetrievalFlightServer"

# JVM选项
JVM_OPTS="-Xms1g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOGS_DIR --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.annotation=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.lang.module=ALL-UNNAMED --add-opens java.base/java.lang.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util.concurrent.locks=ALL-UNNAMED --add-opens java.base/java.util.function=ALL-UNNAMED --add-opens java.base/java.util.jar=ALL-UNNAMED --add-opens java.base/java.util.regex=ALL-UNNAMED --add-opens java.base/java.util.stream=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED --add-opens java.base/java.util.spi=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --enable-preview --add-modules jdk.incubator.vector"

# 应用选项
APP_OPTS=""

# 创建日志目录（如果不存在）
mkdir -p "$LOGS_DIR"

# -----------------------------------------------------------------------------
# 功能函数
# -----------------------------------------------------------------------------

# 检查服务是否运行
is_running() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null; then
            return 0  # 正在运行
        fi
    fi
    return 1  # 未运行
}

# 构建包含libs目录中所有jar的类路径
build_classpath() {
    local classpath=""
    for jar in "$LIBS_DIR"/*.jar; do
        [ -e "$jar" ] || continue
        if [ -z "$classpath" ]; then
            classpath="$jar"
        else
            classpath="$classpath:$jar"
        fi
    done
    echo "$classpath"
}

# 启动服务
start_service() {
    if is_running; then
        echo "服务已在运行中，PID为 $(cat "$PID_FILE")"
        return
    fi

    local classpath=$(build_classpath)
    
    echo "正在启动服务..."
    echo "使用JAVA_HOME: $JAVA_HOME"
    echo "使用类路径: $classpath"
    
    nohup "$JAVA_CMD" $JVM_OPTS -cp "$classpath" "$MAIN_CLASS" $APP_OPTS > "$LOG_FILE" 2> "$ERROR_LOG" &
    
    local pid=$!
    echo $pid > "$PID_FILE"
    
    # 检查进程是否成功启动
    sleep 2
    if ps -p $pid > /dev/null; then
        echo "服务成功启动，PID为 $pid"
    else
        echo "服务启动失败，请查看日志 $ERROR_LOG"
        rm -f "$PID_FILE"
        return 1
    fi
}

# 停止服务
stop_service() {
    if ! is_running; then
        echo "服务未运行"
        return
    fi
    
    local pid=$(cat "$PID_FILE")
    echo "正在停止PID为 $pid 的服务..."
    
    kill $pid
    
    # 等待进程终止
    local timeout=30
    local count=0
    while ps -p $pid > /dev/null && [ $count -lt $timeout ]; do
        sleep 1
        count=$((count+1))
    done
    
    if ps -p $pid > /dev/null; then
        echo "服务在 $timeout 秒后未正常停止，强制终止..."
        kill -9 $pid
        sleep 2
    fi
    
    if ! ps -p $pid > /dev/null; then
        echo "服务已成功停止"
        rm -f "$PID_FILE"
    else
        echo "停止服务失败"
        return 1
    fi
}

# 重启服务
restart_service() {
    stop_service
    sleep 2
    start_service
}

# 显示服务状态
show_status() {
    if is_running; then
        local pid=$(cat "$PID_FILE")
        echo "服务正在运行，PID为 $pid"
        # 显示额外信息如运行时间
        if command -v ps > /dev/null && command -v grep > /dev/null; then
            echo "进程详情:"
            ps -o pid,ppid,user,stat,args -p $pid | grep -v "grep"
        fi
    else
        echo "服务未运行"
        [ -f "$PID_FILE" ] && echo "存在过期的PID文件，正在清理..." && rm -f "$PID_FILE"
    fi
}

# 显示日志
show_logs() {
    local tail_opts="-n 100"
    if [ "$1" = "follow" ]; then
        tail_opts="-f"
    fi
    
    if [ -f "$LOG_FILE" ]; then
        tail $tail_opts "$LOG_FILE"
    else
        echo "未找到日志文件: $LOG_FILE"
    fi
}

# 显示错误日志
show_error_logs() {
    local tail_opts="-n 100"
    if [ "$1" = "follow" ]; then
        tail_opts="-f"
    fi
    
    if [ -f "$ERROR_LOG" ]; then
        tail $tail_opts "$ERROR_LOG"
    else
        echo "未找到错误日志文件: $ERROR_LOG"
    fi
}

# 显示使用信息
show_usage() {
    echo "用法: $0 {start|stop|restart|status|logs|errorlogs|help}"
    echo ""
    echo "命令:"
    echo "  start      启动服务"
    echo "  stop       停止服务"
    echo "  restart    重启服务"
    echo "  status     检查服务状态"
    echo "  logs       显示最近100行日志"
    echo "  logs -f    持续跟踪日志"
    echo "  errorlogs  显示最近100行错误日志"
    echo "  errorlogs -f 持续跟踪错误日志"
    echo "  help       显示此帮助信息"
}

# -----------------------------------------------------------------------------
# 脚本主执行部分
# -----------------------------------------------------------------------------

# 验证JAVA_HOME
if [ ! -x "$JAVA_CMD" ]; then
    echo "错误: JAVA_HOME设置不正确。无法在 $JAVA_CMD 找到Java可执行文件"
    exit 1
fi

# 检查libs目录是否存在并包含jar文件
if [ ! -d "$LIBS_DIR" ] || [ -z "$(ls -A "$LIBS_DIR"/*.jar 2>/dev/null)" ]; then
    echo "错误: 在 $LIBS_DIR 中未找到JAR文件"
    exit 1
fi

# 处理命令行参数
case "$1" in
    start)
        start_service
        ;;
    stop)
        stop_service
        ;;
    restart)
        restart_service
        ;;
    status)
        show_status
        ;;
    logs)
        if [ "$2" = "-f" ]; then
            show_logs "follow"
        else
            show_logs
        fi
        ;;
    errorlogs)
        if [ "$2" = "-f" ]; then
            show_error_logs "follow"
        else
            show_error_logs
        fi
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        echo "未知命令: $1"
        show_usage
        exit 1
        ;;
esac

exit 0 