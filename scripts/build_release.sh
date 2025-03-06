#!/bin/bash

# -----------------------------------------------------------------------------
# 发布打包脚本 - 支持多平台构建
# -----------------------------------------------------------------------------

export JAVA_HOME=/Users/allwefantasy/Library/Java/JavaVirtualMachines/openjdk-21/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

# 解析脚本目录和基础目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
RELEASES_DIR="$BASE_DIR/releases"
JDKS_DIR="$BASE_DIR/releases/jdks"

# 支持的操作系统和CPU平台
SUPPORTED_OS=("linux-x64" "linux-aarch64" "macos-x64" "macos-aarch64" "windows-x64")
DEFAULT_JDK_VERSION="21.0.2"

# 获取当前系统类型
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if [[ $(uname -m) == "aarch64" ]]; then
            echo "linux-aarch64"
        else
            echo "linux-x64"
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        if [[ $(uname -m) == "arm64" ]]; then
            echo "macos-aarch64"
        else
            echo "macos-x64"
        fi
    elif [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
        echo "windows-x64"
    else
        echo "unknown"
    fi
}

CURRENT_OS=$(detect_os)

# 帮助函数
show_help() {
    echo "用法: $0 [选项] [版本号]"
    echo ""
    echo "选项:"
    echo "  -o, --os OS            指定构建的操作系统 (默认: $CURRENT_OS)"
    echo "                         支持的系统: linux-x64, linux-aarch64, macos-x64, macos-aarch64, windows-x64"
    echo "  -j, --jdk-version VER  指定JDK版本 (默认: $DEFAULT_JDK_VERSION)"
    echo "  -a, --all              为所有支持的平台构建发布包"
    echo "  -h, --help             显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 1.0.0                   # 为当前平台构建版本1.0.0"
    echo "  $0 -o linux-x64 1.0.0      # 为Linux/x64构建版本1.0.0"
    echo "  $0 -a 1.0.0                # 为所有平台构建版本1.0.0"
}

# 解析命令行参数
BUILD_OS="$CURRENT_OS"
BUILD_ALL=false
JDK_VERSION="$DEFAULT_JDK_VERSION"

while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--os)
            BUILD_OS="$2"
            shift 2
            ;;
        -j|--jdk-version)
            JDK_VERSION="$2"
            shift 2
            ;;
        -a|--all)
            BUILD_ALL=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        -*)
            echo "错误: 未知选项 $1"
            show_help
            exit 1
            ;;
        *)
            VERSION="$1"
            shift
            ;;
    esac
done

# 获取版本号
if [ -z "$VERSION" ]; then
    # 如果未提供版本号参数，尝试从pom.xml中获取
    if [ -f "$BASE_DIR/pom.xml" ]; then
        VERSION=$(grep -m 1 "<version>" "$BASE_DIR/pom.xml" | sed -e 's/<version>\(.*\)<\/version>/\1/' | tr -d '[:space:]')
    else
        echo "错误: 版本号未提供，且无法从pom.xml获取"
        echo "用法: $0 [选项] <版本号>"
        exit 1
    fi
fi

# 验证JDK目录
check_jdk_exists() {
    local os_type=$1
    local jdk_path="$JDKS_DIR/$os_type/jdk-$JDK_VERSION"
    
    if [ ! -d "$jdk_path" ]; then
        echo "警告: 未找到 $os_type 平台的JDK $JDK_VERSION"
        echo "请先运行: $SCRIPT_DIR/download_jdk.sh -o $os_type -v $JDK_VERSION"
        return 1
    fi
    
    return 0
}

# 为指定平台构建发布包
build_for_platform() {
    local os_type=$1
    local app_version=$2
    
    # 检查是否有对应平台的JDK
    if ! check_jdk_exists "$os_type"; then
        echo "错误: 缺少 $os_type 平台的JDK，跳过此平台的构建"
        return 1
    fi
    
    # 设置发布目录
    local RELEASE_NAME="byzer-storage-$app_version-$os_type"
    local RELEASE_DIR="$RELEASES_DIR/$RELEASE_NAME"
    local RELEASE_BIN_DIR="$RELEASE_DIR/bin"
    local RELEASE_LIBS_DIR="$RELEASE_DIR/libs"
    local RELEASE_JDK_DIR="$RELEASE_DIR/jdk"
    
    echo "===================================="
    echo "开始构建 $RELEASE_NAME"
    echo "===================================="
    
    # 检查是否已存在相同版本的 release，如果存在则删除
    if [ -d "$RELEASE_DIR" ]; then
        echo "发现已存在的发布目录: $RELEASE_DIR"
        echo "正在删除已有发布..."
        rm -rf "$RELEASE_DIR"
    fi
    
    # 检查是否已存在相同版本的压缩包，如果存在则删除
    if [ -f "$RELEASES_DIR/$RELEASE_NAME.tar.gz" ]; then
        echo "发现已存在的发布压缩包: $RELEASES_DIR/$RELEASE_NAME.tar.gz"
        echo "正在删除已有压缩包..."
        rm -f "$RELEASES_DIR/$RELEASE_NAME.tar.gz"
    fi
    
    # 创建目录结构
    echo "创建发布目录结构..."
    mkdir -p "$RELEASE_BIN_DIR"
    mkdir -p "$RELEASE_LIBS_DIR"
    mkdir -p "$RELEASE_JDK_DIR"
    mkdir -p "$RELEASE_DIR/logs"
    
    # 仅在第一个平台构建时执行Maven构建
    if [ "$FIRST_BUILD" = true ] || [ "$BUILD_ALL" = false ]; then
        # 使用Maven构建项目
        echo "使用Maven构建项目..."
        cd "$BASE_DIR"
        mvn clean package -DskipTests

        # 复制依赖JAR包
        mvn dependency:copy-dependencies -DoutputDirectory=target/dependencies        
        
        # 检查Maven构建是否成功
        if [ $? -ne 0 ]; then
            echo "Maven构建失败，终止发布流程"
            exit 1
        fi
        
        # 标记已完成第一次构建
        FIRST_BUILD=false
    else
        echo "跳过Maven构建 (已在之前的平台构建中完成)"
    fi
    
    
    # 复制主JAR包
    cp "$BASE_DIR/target/"*.jar "$RELEASE_LIBS_DIR/"
    
    # 复制依赖JAR包
    cp "$BASE_DIR/target/dependencies"/*.jar "$RELEASE_LIBS_DIR/"
    
    # 复制启动脚本
    echo "复制服务管理脚本..."
    cp "$SCRIPT_DIR/service.sh" "$RELEASE_BIN_DIR/"
    cp "$SCRIPT_DIR/service.bat" "$RELEASE_BIN_DIR/"
    chmod +x "$RELEASE_BIN_DIR/service.sh"
    chmod +x "$RELEASE_BIN_DIR/service.bat"
    # 拷贝对应平台的JDK
    echo "复制 $os_type 平台的JDK..."
    JDK_SOURCE_DIR="$JDKS_DIR/$os_type/jdk-$JDK_VERSION"
    cp -r "$JDK_SOURCE_DIR/"* "$RELEASE_JDK_DIR/"
    
    # 创建一个README文件
    cat > "$RELEASE_DIR/README.txt" << EOF
Byzer-Storage $app_version ($os_type)
====================

目录结构:
- bin/: 包含启动和管理脚本
- libs/: 包含应用程序JAR包和依赖
- jdk/: 包含运行环境所需的JDK $JDK_VERSION
- logs/: 应用程序日志目录

使用方法:
- 启动: bin/service.sh start
- 停止: bin/service.sh stop
- 查看状态: bin/service.sh status
- 查看日志: bin/service.sh logs
EOF
    
    # 创建发布包
    echo "创建发布包..."
    cd "$RELEASES_DIR"
    tar -czf "$RELEASE_NAME.tar.gz" "$(basename "$RELEASE_DIR")"
    
    echo "===================================="
    echo "平台 $os_type 的发布构建完成!"
    echo "发布目录: $RELEASE_DIR"
    echo "发布包: $RELEASES_DIR/$RELEASE_NAME.tar.gz"
    echo "===================================="
    
    return 0
}

# 确保releases目录存在
mkdir -p "$RELEASES_DIR"

# 初始化构建标记
FIRST_BUILD=true
FIRST_PLATFORM_LIBS_DIR=""

# 根据选项构建单平台或全平台
if [ "$BUILD_ALL" = true ]; then
    echo "准备为所有支持的平台构建发布包..."
    
    # 首先检查所有平台的JDK是否存在
    MISSING_JDK=false
    for os_type in "${SUPPORTED_OS[@]}"; do
        if ! check_jdk_exists "$os_type"; then
            MISSING_JDK=true
        fi
    done
    
    if [ "$MISSING_JDK" = true ]; then
        echo "检测到缺少部分平台的JDK"
        echo "是否继续构建已有JDK的平台? (y/n)"
        read -r answer
        if [[ ! "$answer" =~ ^[Yy]$ ]]; then
            echo "已取消构建"
            exit 1
        fi
    fi
    
    # 构建所有平台
    BUILD_COUNT=0
    SUCCESS_COUNT=0
    
    for os_type in "${SUPPORTED_OS[@]}"; do
        echo ""
        echo "正在为平台 $os_type 构建..."
        
        # 检查JDK是否存在
        if check_jdk_exists "$os_type"; then
            BUILD_COUNT=$((BUILD_COUNT+1))
            if build_for_platform "$os_type" "$VERSION"; then
                SUCCESS_COUNT=$((SUCCESS_COUNT+1))
            fi
        else
            echo "跳过 $os_type 平台 (缺少JDK)"
        fi
    done
    
    # 显示汇总信息
    echo ""
    echo "===================================="
    echo "所有平台构建完成"
    echo "总计尝试: $BUILD_COUNT"
    echo "成功构建: $SUCCESS_COUNT"
    echo "===================================="
else
    # 构建单个平台
    build_for_platform "$BUILD_OS" "$VERSION"
fi

exit 0 