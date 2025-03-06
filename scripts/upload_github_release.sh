#!/bin/bash

# -----------------------------------------------------------------------------
# GitHub Release Upload Script - 自动将构建好的发布包上传到GitHub Releases
# -----------------------------------------------------------------------------

set -e

# 解析脚本目录和基础目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
RELEASES_DIR="$BASE_DIR/releases"

# 默认设置
DEFAULT_REPO="allwefantasy/BYZER-RETRIEVAL"

# 帮助函数
show_help() {
    echo "用法: $0 [选项] <版本号>"
    echo ""
    echo "选项:"
    echo "  -r, --repo REPO      GitHub仓库，格式为 '用户名/仓库名' (默认: $DEFAULT_REPO)"
    echo "  -t, --token TOKEN    GitHub Personal Access Token，必需"
    echo "  -d, --draft          创建草稿版本 (默认: false)"
    echo "  -p, --prerelease     标记为预发布 (默认: false)"
    echo "  -m, --message MSG    发布说明 (默认: 自动生成)"
    echo "  -h, --help           显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 -t ghp_xxxx 1.0.0             # 上传版本1.0.0到默认仓库"
    echo "  $0 -r myuser/myrepo -t ghp_xxxx 1.0.0  # 上传到指定仓库"
    echo ""
    echo "注意: 请确保GitHub Token具有 'repo' 权限"
}

# 解析命令行参数
GITHUB_REPO="$DEFAULT_REPO"
GITHUB_TOKEN=""
IS_DRAFT="false"
IS_PRERELEASE="false"
RELEASE_MESSAGE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--repo)
            GITHUB_REPO="$2"
            shift 2
            ;;
        -t|--token)
            GITHUB_TOKEN="$2"
            shift 2
            ;;
        -d|--draft)
            IS_DRAFT="true"
            shift
            ;;
        -p|--prerelease)
            IS_PRERELEASE="true"
            shift
            ;;
        -m|--message)
            RELEASE_MESSAGE="$2"
            shift 2
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

# 检查必要参数
if [ -z "$VERSION" ]; then
    echo "错误: 缺少版本号"
    show_help
    exit 1
fi

if [ -z "$GITHUB_TOKEN" ]; then
    echo "错误: 必须提供GitHub Token"
    show_help
    exit 1
fi

# 检查releases目录是否存在
if [ ! -d "$RELEASES_DIR" ]; then
    echo "错误: 发布目录不存在: $RELEASES_DIR"
    echo "请先运行 build_release.sh 脚本构建发布包"
    exit 1
fi

# 查找对应版本的发布包
PACKAGES=($(find "$RELEASES_DIR" -name "byzer-storage-${VERSION}-*.tar.gz"))

if [ ${#PACKAGES[@]} -eq 0 ]; then
    echo "错误: 未找到版本 $VERSION 的发布包"
    echo "请先运行 build_release.sh 脚本构建发布包"
    exit 1
fi

echo "找到 ${#PACKAGES[@]} 个发布包:"
for pkg in "${PACKAGES[@]}"; do
    echo "  - $(basename "$pkg")"
done

# 生成发布说明（如果未提供）
if [ -z "$RELEASE_MESSAGE" ]; then
    RELEASE_MESSAGE="# Byzer Storage $VERSION 发布

## 发布包

此版本包含以下平台的发布包:
"

    for pkg in "${PACKAGES[@]}"; do
        pkg_name=$(basename "$pkg")
        RELEASE_MESSAGE+="- $pkg_name
"
    done

    RELEASE_MESSAGE+="
## 安装说明

1. 下载适合您平台的发布包
2. 解压: \`tar -xzf byzer-storage-$VERSION-*.tar.gz\`
3. 启动服务: \`cd byzer-storage-$VERSION-* && bin/service.sh start\`

详细文档请参阅 [Byzer Storage 文档](https://github.com/$GITHUB_REPO)"
fi

# 创建GitHub Release
echo "正在创建GitHub Release..."

# 使用临时文件处理JSON，避免复杂的转义问题
TEMP_JSON_FILE=$(mktemp)
cat > "$TEMP_JSON_FILE" << EOF
{
  "tag_name": "v$VERSION",
  "target_commitish": "main",
  "name": "v$VERSION",
  "body": $(echo "$RELEASE_MESSAGE" | jq -Rs .),
  "draft": $IS_DRAFT,
  "prerelease": $IS_PRERELEASE
}
EOF

RESPONSE=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github.v3+json" \
    -H "Content-Type: application/json" \
    -X POST "https://api.github.com/repos/$GITHUB_REPO/releases" \
    --data @"$TEMP_JSON_FILE")

# 清理临时文件
rm -f "$TEMP_JSON_FILE"

# 检查是否创建成功
RELEASE_ID=$(echo "$RESPONSE" | grep -o '"id": [0-9]*' | head -1 | sed 's/"id": //')

if [ -z "$RELEASE_ID" ]; then
    echo "错误: 创建Release失败"
    echo "API响应: $RESPONSE"
    exit 1
fi

echo "成功创建Release，ID: $RELEASE_ID"

# 上传发布包
for pkg in "${PACKAGES[@]}"; do
    pkg_name=$(basename "$pkg")
    echo "正在上传 $pkg_name..."
    
    UPLOAD_URL="https://uploads.github.com/repos/$GITHUB_REPO/releases/$RELEASE_ID/assets?name=$pkg_name"
    
    curl -s -H "Authorization: token $GITHUB_TOKEN" \
        -H "Accept: application/vnd.github.v3+json" \
        -H "Content-Type: application/octet-stream" \
        -X POST "$UPLOAD_URL" \
        --data-binary @"$pkg"
        
    if [ $? -eq 0 ]; then
        echo "  上传成功"
    else
        echo "  上传失败"
    fi
done

echo "===================================="
echo "GitHub Release 创建并上传完成!"
echo "版本: v$VERSION"
echo "发布状态: $([ "$IS_DRAFT" = true ] && echo "草稿" || echo "已发布")"
echo "预发布: $([ "$IS_PRERELEASE" = true ] && echo "是" || echo "否")"
echo "查看链接: https://github.com/$GITHUB_REPO/releases/tag/v$VERSION"
echo "===================================="

exit 0 