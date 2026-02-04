#!/bin/bash

# Configuration
VERSION="1.0.0.0"
MPACK_NAME="varga-mpack"
SOURCE_DIR="../mpack-varga"
OUTPUT_DIR="target"
PROJECT_ROOT="../.."

# Repositories URLs (adjust if needed)
KIRKA_REPO="https://github.com/varga/kirka.git" # Example
TARN_REPO="https://github.com/varga/tarn.git"   # Example

echo "Building $MPACK_NAME version $VERSION..."

# Clone or update repositories
build_project() {
    local name=$1
    local repo_url=$2
    local dir="$PROJECT_ROOT/$name"
    
    if [ ! -d "$dir" ]; then
        echo "Cloning $name..."
        git clone "$repo_url" "$dir"
    else
        echo "Updating $name..."
        cd "$dir" && git pull && cd - > /dev/null
    fi
    
    echo "Building $name..."
    cd "$dir"
    ./mvnw clean package -DskipTests # Assuming mvnw is present
    cd - > /dev/null
}

# 1. Build Kirka and Tarn
build_project "kirka" "$KIRKA_REPO"
build_project "tarn" "$TARN_REPO"

# 2. Copy JARs to mpack
echo "Copying JARs to mpack..."
KIRKA_MPACK_FILES="$SOURCE_DIR/common-services/KIRKA/1.0.0/package/files"
TARN_MPACK_FILES="$SOURCE_DIR/common-services/TARN/1.0.0/package/files"

# Ensure target directories exist and are empty
rm -rf "$KIRKA_MPACK_FILES"
rm -rf "$TARN_MPACK_FILES"
mkdir -p "$KIRKA_MPACK_FILES"
mkdir -p "$TARN_MPACK_FILES"

cp "$PROJECT_ROOT/kirka/target/kirka-"*.jar "$KIRKA_MPACK_FILES/kirka.jar"
cp "$PROJECT_ROOT/tarn/target/tarn-"*.jar "$TARN_MPACK_FILES/tarn.jar"

# Cleanup build directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Create archive
echo "Creating mpack archive..."
cd $SOURCE_DIR
tar -cvzf ../build/$OUTPUT_DIR/$MPACK_NAME-$VERSION.tar.gz .

echo "Build complete: build/$OUTPUT_DIR/$MPACK_NAME-$VERSION.tar.gz"
