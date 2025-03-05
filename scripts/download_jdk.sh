#!/bin/bash

# -----------------------------------------------------------------------------
# JDK下载脚本 - 下载OpenJDK 21并解压到指定目录
# -----------------------------------------------------------------------------

# 解析脚本目录和基础目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

# 默认JDK版本和目标目录
DEFAULT_VERSION="21.0.2"
DEFAULT_TARGET_DIR="$BASE_DIR/releases/jdks/$DEFAULT_OS/jdk-$DEFAULT_VERSION"

# 支持的JDK版本和操作系统
SUPPORTED_VERSIONS=("21.0.2")
SUPPORTED_OS=("linux-x64" "linux-aarch64" "macos-x64" "macos-aarch64" "windows-x64")

# 默认检测操作系统类型
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

DEFAULT_OS=$(detect_os)

# 更新为使用新检测的OS
DEFAULT_TARGET_DIR="$BASE_DIR/releases/jdks/$DEFAULT_OS/jdk-$DEFAULT_VERSION"

# 帮助函数
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -v, --version VERSION   指定JDK版本 (默认: $DEFAULT_VERSION)"
    echo "                          支持的版本: 21.0.2, 21.0.1, 21"
    echo "  -o, --os OS             指定操作系统 (默认: 自动检测为 $DEFAULT_OS)"
    echo "                          支持的值: linux-x64, linux-aarch64, macos-x64, macos-aarch64, windows-x64"
    echo "  -d, --dir DIRECTORY     指定JDK安装目录 (默认: $DEFAULT_TARGET_DIR)"
    echo "  -a, --all               下载所有支持的JDK版本和操作系统"
    echo "  -h, --help              显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                                # 下载默认版本 $DEFAULT_VERSION 到默认目录"
    echo "  $0 -v 21.0.1                     # 下载版本 21.0.1"
    echo "  $0 -o linux-x64 -d /opt/jdk21    # 为Linux/x64下载并安装到/opt/jdk21"
    echo "  $0 -a                            # 下载所有版本和所有支持的操作系统"
}

# 解析命令行参数
VERSION="$DEFAULT_VERSION"
OS="$DEFAULT_OS"
TARGET_DIR="$DEFAULT_TARGET_DIR"
DOWNLOAD_ALL=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -o|--os)
            OS="$2"
            # 更新目标目录以反映指定的OS
            TARGET_DIR="$BASE_DIR/releases/jdks/$OS/jdk-$VERSION"
            shift 2
            ;;
        -d|--dir)
            TARGET_DIR="$2"
            shift 2
            ;;
        -a|--all)
            DOWNLOAD_ALL=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "错误: 未知选项 $1"
            show_help
            exit 1
            ;;
    esac
done

# 根据版本和操作系统确定下载URL
get_download_info() {
    local version=$1
    local os=$2
    
    # 不同版本的下载URL和SHA256校验和
    case $version in
        "21.0.2")
            local build="21.0.2+13"
            local build_path="f2283984656d49d69e91c558476027ac/13"
            ;;
        "21.0.1")
            local build="21.0.1+12"
            local build_path="415e3f918a1f4062a0074a2794853d0d/12"
            ;;
        "21")
            local build="21+35"
            local build_path="fd2272bbf8e04c3dbaee13770090416c/35"
            ;;
        *)
            echo "错误: 不支持的JDK版本: $version"
            echo "支持的版本: 21.0.2, 21.0.1, 21"
            exit 1
            ;;
    esac
    
    # 根据操作系统确定文件名和扩展名
    case $os in
        "windows-x64")
            local filename="openjdk-${version}_windows-x64_bin.zip"
            local ext="zip"
            ;;
        "linux-x64")
            local filename="openjdk-${version}_linux-x64_bin.tar.gz"
            local ext="tar.gz"
            ;;
        "linux-aarch64")
            local filename="openjdk-${version}_linux-aarch64_bin.tar.gz"
            local ext="tar.gz"
            ;;
        "macos-x64")
            local filename="openjdk-${version}_macos-x64_bin.tar.gz"
            local ext="tar.gz"
            ;;
        "macos-aarch64")
            local filename="openjdk-${version}_macos-aarch64_bin.tar.gz"
            local ext="tar.gz"
            ;;
    esac
    
    local url="https://download.java.net/java/GA/jdk${version}/${build_path}/GPL/${filename}"
    local sha_url="${url}.sha256"
    
    echo "$url $sha_url $filename $ext"
}

# 下载并安装单个JDK版本
download_and_install_jdk() {
    local VERSION=$1
    local OS=$2
    local TARGET_DIR=$3

    # 检查目标目录是否已存在，如果存在则跳过
    if [ -d "$TARGET_DIR" ]; then
        echo "目标目录 $TARGET_DIR 已存在。跳过此版本。"
        return 0
    fi

    # 检查OS值是否有效
    if [[ "$OS" != "linux-x64" && "$OS" != "linux-aarch64" && "$OS" != "macos-x64" && "$OS" != "macos-aarch64" && "$OS" != "windows-x64" ]]; then
        echo "错误: 不支持的操作系统类型: $OS"
        echo "支持的类型: linux-x64, linux-aarch64, macos-x64, macos-aarch64, windows-x64"
        return 1
    fi

    # 获取下载信息
    DOWNLOAD_INFO=$(get_download_info "$VERSION" "$OS")
    URL=$(echo "$DOWNLOAD_INFO" | cut -d' ' -f1)
    SHA_URL=$(echo "$DOWNLOAD_INFO" | cut -d' ' -f2)
    FILENAME=$(echo "$DOWNLOAD_INFO" | cut -d' ' -f3)
    EXT=$(echo "$DOWNLOAD_INFO" | cut -d' ' -f4)

    # 创建临时目录
    TEMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TEMP_DIR"' EXIT

    # 下载JDK
    echo "===================================="
    echo "开始下载 OpenJDK $VERSION 为 $OS"
    echo "下载地址: $URL"
    echo "目标目录: $TARGET_DIR"
    echo "===================================="

    # 创建目标目录
    mkdir -p "$TARGET_DIR"

    echo "下载JDK..."
    if command -v curl &>/dev/null; then
        curl -# -L -o "$TEMP_DIR/$FILENAME" "$URL"
        CURL_EXIT=$?
        if [ $CURL_EXIT -ne 0 ]; then
            echo "错误: 下载失败! curl退出代码: $CURL_EXIT"
            return 1
        fi
    elif command -v wget &>/dev/null; then
        wget -q --show-progress -O "$TEMP_DIR/$FILENAME" "$URL"
        WGET_EXIT=$?
        if [ $WGET_EXIT -ne 0 ]; then
            echo "错误: 下载失败! wget退出代码: $WGET_EXIT"
            return 1
        fi
    else
        echo "错误: 需要curl或wget下载文件，但这两个命令都不可用。"
        return 1
    fi

    # 验证校验和
    echo "验证下载..."
    if command -v curl &>/dev/null; then
        EXPECTED_SHA256=$(curl -s "$SHA_URL" | awk '{print $1}')
    elif command -v wget &>/dev/null; then
        EXPECTED_SHA256=$(wget -q -O - "$SHA_URL" | awk '{print $1}')
    fi

    if command -v shasum &>/dev/null; then
        ACTUAL_SHA256=$(shasum -a 256 "$TEMP_DIR/$FILENAME" | awk '{print $1}')
    elif command -v sha256sum &>/dev/null; then
        ACTUAL_SHA256=$(sha256sum "$TEMP_DIR/$FILENAME" | awk '{print $1}')
    else
        echo "警告: 无法验证校验和，找不到shasum或sha256sum命令"
        ACTUAL_SHA256="skip-verification"
        EXPECTED_SHA256="skip-verification"
    fi

    if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ] && [ "$EXPECTED_SHA256" != "skip-verification" ]; then
        echo "错误: 校验和验证失败!"
        echo "预期: $EXPECTED_SHA256"
        echo "实际: $ACTUAL_SHA256"
        return 1
    fi

    # 解压JDK
    echo "解压JDK到 $TARGET_DIR..."
    case $EXT in
        "tar.gz")
            tar -xzf "$TEMP_DIR/$FILENAME" -C "$TEMP_DIR"
            ;;
        "zip")
            if command -v unzip &>/dev/null; then
                unzip -q "$TEMP_DIR/$FILENAME" -d "$TEMP_DIR"
            else
                echo "错误: 需要unzip命令解压zip文件，但该命令不可用。"
                return 1
            fi
            ;;
    esac

    # 查找解压后的JDK目录
    JDK_EXTRACTED_DIR=$(find "$TEMP_DIR" -maxdepth 1 -type d -name "jdk*" | head -n 1)
    if [ -z "$JDK_EXTRACTED_DIR" ]; then
        echo "错误: 无法找到解压后的JDK目录"
        return 1
    fi

    # 复制JDK文件到目标目录
    echo "复制JDK文件到目标目录..."
    cp -r "$JDK_EXTRACTED_DIR"/* "$TARGET_DIR/"

    # 设置可执行权限
    chmod +x "$TARGET_DIR/bin/"*

    echo "===================================="
    echo "JDK $VERSION 下载并安装成功!"
    echo "安装目录: $TARGET_DIR"
    echo "使用示例: $TARGET_DIR/bin/java -version"
    echo "===================================="

    return 0
}

# 是否下载所有版本和平台
if [ "$DOWNLOAD_ALL" = true ]; then
    echo "准备下载所有JDK版本和操作系统..."
    
    # 创建基本目录
    BASE_JDK_DIR="$BASE_DIR/releases/jdks"
    mkdir -p "$BASE_JDK_DIR"
    
    # 记录总数和成功数
    TOTAL_COUNT=0
    SUCCESS_COUNT=0
    
    # 遍历所有版本和操作系统组合
    for os_type in "${SUPPORTED_OS[@]}"; do
        for ver in "${SUPPORTED_VERSIONS[@]}"; do
            # 设置目标目录 - 按操作系统和版本组织
            TARGET_SUBDIR="$BASE_JDK_DIR/$os_type/jdk-$ver"
            
            echo ""
            echo "正在处理: JDK $ver 为 $os_type"
            
            # 检查目录是否已存在
            if [ -d "$TARGET_SUBDIR" ]; then
                echo "目录 $TARGET_SUBDIR 已存在。跳过此版本。"
                continue
            fi
            
            # 下载和安装特定版本和平台
            TOTAL_COUNT=$((TOTAL_COUNT+1))
            if download_and_install_jdk "$ver" "$os_type" "$TARGET_SUBDIR"; then
                SUCCESS_COUNT=$((SUCCESS_COUNT+1))
            fi
        done
    done
    
    # 显示汇总信息
    echo ""
    echo "===================================="
    echo "所有下载任务完成"
    echo "总计尝试: $TOTAL_COUNT"
    echo "成功下载: $SUCCESS_COUNT"
    echo "下载目录: $BASE_JDK_DIR/<os>/jdk-<version>"
    echo "===================================="
else
    # 单个版本和平台的下载 - 确保目标目录按OS组织
    if [ "$TARGET_DIR" = "$DEFAULT_TARGET_DIR" ]; then
        # 如果用户没有手动指定目录，使用标准目录结构
        TARGET_DIR="$BASE_DIR/releases/jdks/$OS/jdk-$VERSION"
    fi
    download_and_install_jdk "$VERSION" "$OS" "$TARGET_DIR"
fi

exit 0 