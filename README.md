# Byzer-Storage

Byzer-Storage 是一个存储服务项目，支持多平台部署。

## 构建与发布指南

本项目提供了两个核心脚本来简化跨平台的JDK下载和项目构建发布流程：`download_jdk.sh`和`build_release.sh`。

### JDK下载工具 (download_jdk.sh)

#### 功能特点

- 自动检测当前操作系统和架构
- 支持多种操作系统平台：Linux (x64/ARM64)、macOS (x64/ARM64)、Windows
- 支持JDK 21.0.2版本
- 自动验证下载文件的SHA256校验和
- 可以批量下载所有平台的JDK

#### 使用方法

```bash
./scripts/download_jdk.sh [选项]
```

#### 可用选项

| 选项 | 说明 |
|------|------|
| `-v, --version VERSION` | 指定要下载的JDK版本 (默认: 21.0.2) |
| `-o, --os OS` | 指定目标操作系统/平台 (默认: 自动检测) |
| `-d, --dir DIRECTORY` | 指定JDK安装目录 |
| `-a, --all` | 下载所有支持的平台的JDK |
| `-h, --help` | 显示帮助信息 |

#### 支持的平台

- linux-x64: Linux x86_64
- linux-aarch64: Linux ARM64
- macos-x64: macOS Intel
- macos-aarch64: macOS Apple Silicon (M1/M2)
- windows-x64: Windows 64位

#### 示例用法

```bash
# 下载JDK 21.0.2到默认目录
./scripts/download_jdk.sh

# 下载到指定目录
./scripts/download_jdk.sh -d /path/to/jdk

# 下载特定平台的JDK
./scripts/download_jdk.sh -o linux-x64

# 下载所有平台的JDK (批量下载)
./scripts/download_jdk.sh -a
```

### 发布构建工具 (build_release.sh)

#### 功能特点

- 构建项目并打包为特定平台的发布包
- 自动集成对应平台的JDK
- 支持单一平台构建或所有平台一次性构建
- 智能检测和复用依赖以加速多平台构建

#### 使用方法

```bash
./scripts/build_release.sh [选项] [版本号]
```

#### 可用选项

| 选项 | 说明 |
|------|------|
| `-o, --os OS` | 指定构建的目标平台 (默认: 当前平台) |
| `-j, --jdk-version VER` | 指定要使用的JDK版本 (默认: 21.0.2) |
| `-a, --all` | 为所有支持的平台构建发布包 |
| `-h, --help` | 显示帮助信息 |

#### 示例用法

```bash
# 为当前平台构建
./scripts/build_release.sh 1.0.0

# 为特定平台构建
./scripts/build_release.sh -o linux-x64 1.0.0

# 使用特定JDK版本构建
./scripts/build_release.sh -j 21.0.2 1.0.0

# 为所有平台构建
./scripts/build_release.sh -a 1.0.0
```

#### 发布包内容

每个生成的发布包都具有以下结构：

```
byzer-storage-<版本>-<平台>/
├── bin/           # 包含服务管理脚本
├── jdk/           # 包含对应平台的JDK
├── libs/          # 包含应用JAR和所有依赖
└── logs/          # 日志目录
```

### 典型工作流程

完整的构建和发布流程通常包括以下步骤：

1. 首先下载所需的JDK：
   ```bash
   ./scripts/download_jdk.sh -a
   ```

2. 然后为所有平台构建发布包：
   ```bash
   ./scripts/build_release.sh -a 1.0.0
   ```

3. 发布包将生成在`releases/`目录下，格式为：
   ```
   byzer-storage-1.0.0-<平台>.tar.gz
   ```

### 注意事项

- 在使用`build_release.sh`之前，确保已经通过`download_jdk.sh`下载了相应平台的JDK
- 构建全平台发布包可能需要较长时间，特别是下载所有JDK时
- 这些脚本假设在类Unix环境中运行 (Linux/macOS 或 Windows上的Git Bash/WSL)

