#!/bin/bash

# Check if GitHub CLI is installed
if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is not installed. Please install it first."
    echo "You can install it using: brew install gh"
    exit 1
fi

# Authenticate with GitHub using personal key
echo "${GITHUB_KEY}" | gh auth login --with-token

# Configuration
version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
PROJECT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

VERSION=${version}

JAR_PATH=${PROJECT}/target/byzer-retrieval-lib-${version}.tar.gz
echo "Uploading $JAR_PATH..."

# Check if file exists
if [ ! -f "$JAR_PATH" ]; then
    echo "Error: File $JAR_PATH does not exist!"
    exit 1
fi

# Create a new release
echo "Creating GitHub release $VERSION..."
gh release create "$VERSION" \
    --title "Release $VERSION" \
    --notes "Release $VERSION" \
    "$JAR_PATH" --debug

if [ $? -eq 0 ]; then
    echo "Successfully created release $VERSION and uploaded $JAR_PATH"
else
    echo "Failed to create release"
    exit 1
fi
