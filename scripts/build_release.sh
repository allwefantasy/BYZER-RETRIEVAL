#!/bin/bash

# -----------------------------------------------------------------------------
# 发布打包脚本
# -----------------------------------------------------------------------------

# 解析脚本目录和基础目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
RELEASES_DIR="$BASE_DIR/releases"

# 获取版本号
if [ -z "$1" ]; then
    # 如果未提供版本号参数，尝试从pom.xml中获取
    if [ -f "$BASE_DIR/pom.xml" ]; then
        VERSION=$(grep -m 1 "<version>" "$BASE_DIR/pom.xml" | sed -e 's/<version>\(.*\)<\/version>/\1/' | tr -d '[:space:]')
    else
        echo "错误: 版本号未提供，且无法从pom.xml获取"
        echo "用法: $0 <版本号>"
        exit 1
    fi
else
    VERSION="$1"
fi

# 设置发布目录
RELEASE_NAME="byzer-storage-$VERSION"
RELEASE_DIR="$RELEASES_DIR/$RELEASE_NAME"
RELEASE_BIN_DIR="$RELEASE_DIR/bin"
RELEASE_LIBS_DIR="$RELEASE_DIR/libs"
RELEASE_JDK_DIR="$RELEASE_DIR/jdk"

echo "===================================="
echo "开始构建 $RELEASE_NAME"
echo "===================================="

# 创建目录结构
echo "创建发布目录结构..."
mkdir -p "$RELEASE_BIN_DIR"
mkdir -p "$RELEASE_LIBS_DIR"
mkdir -p "$RELEASE_JDK_DIR"
mkdir -p "$RELEASE_DIR/logs"

# 使用Maven构建项目
echo "使用Maven构建项目..."
cd "$BASE_DIR"
mvn clean package -DskipTests

# 检查Maven构建是否成功
if [ $? -ne 0 ]; then
    echo "Maven构建失败，终止发布流程"
    exit 1
fi

# 复制依赖JAR包
echo "复制JAR包和依赖..."
# 复制主JAR包
cp "$BASE_DIR/target/"*.jar "$RELEASE_LIBS_DIR/"

# 复制依赖JAR包
# 使用Maven依赖插件复制所有依赖到libs目录
mvn dependency:copy-dependencies -DoutputDirectory="$RELEASE_LIBS_DIR" -DincludeScope=runtime

# 复制启动脚本
echo "复制服务管理脚本..."
cp "$SCRIPT_DIR/service.sh" "$RELEASE_BIN_DIR/"
chmod +x "$RELEASE_BIN_DIR/service.sh"

# JDK处理提示
echo "需要手动准备JDK 21并放置在: $RELEASE_JDK_DIR"
echo "您可以从以下位置下载JDK 21: https://jdk.java.net/21/"

# 创建一个README文件
cat > "$RELEASE_DIR/README.txt" << EOF
Byzer-Storage $VERSION
====================

目录结构:
- bin/: 包含启动和管理脚本
- libs/: 包含应用程序JAR包和依赖
- jdk/: 需要手动放置JDK 21
- logs/: 应用程序日志目录

安装步骤:
1. 将JDK 21放置在jdk/目录下
2. 通过运行bin/service.sh启动应用程序

使用方法:
- 启动: bin/service.sh start
- 停止: bin/service.sh stop
- 查看状态: bin/service.sh status
- 查看日志: bin/service.sh logs
EOF

# 创建发布包
echo "创建发布包..."
cd "$RELEASES_DIR"
tar -czf "$RELEASE_NAME.tar.gz" "$RELEASE_NAME"

echo "===================================="
echo "发布构建完成!"
echo "发布目录: $RELEASE_DIR"
echo "发布包: $RELEASES_DIR/$RELEASE_NAME.tar.gz"
echo ""
echo "请记得将JDK 21放置在: $RELEASE_JDK_DIR"
echo "===================================="

exit 0 