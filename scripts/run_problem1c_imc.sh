#!/bin/bash

set -e

echo "======================================="
echo "Running Problem 1(e): STRIPES + IMC (Local)"
echo "======================================="

PROJECT_ROOT="$(pwd)"

SRC_DIR="$PROJECT_ROOT/src"
BUILD_DIR="$PROJECT_ROOT/build"
JAR_NAME="problem1c_imc_local.jar"

INPUT="$PROJECT_ROOT/Data/wiki_all_fixed.txt"
TOP50="$PROJECT_ROOT/top50.txt"
OUTPUT_BASE="$PROJECT_ROOT/output/problem1c_imc_local"

DIST=${1:-1}
OUTPUT="${OUTPUT_BASE}_d${DIST}"

# -------- CLEAN --------
echo "[1] Cleaning..."
rm -rf "$BUILD_DIR" "$OUTPUT"
mkdir -p "$BUILD_DIR"

# -------- COMPILE --------
echo "[2] Compiling..."

javac -classpath "$(hadoop classpath)" -d "$BUILD_DIR" \
    "$SRC_DIR/problem1/Problem1c_Stripes_IMC_Local.java"

# -------- CREATE JAR --------
echo "[3] Creating JAR..."
cd "$BUILD_DIR"
jar -cvf "$JAR_NAME" .
cd "$PROJECT_ROOT"

# -------- CHECK FILES --------
if [ ! -f "$TOP50" ]; then
    echo "ERROR: top50.txt not found!"
    exit 1
fi

# -------- RUN --------
echo "[4] Running job (d=$DIST)..."

START=$(date +%s)

hadoop jar "$BUILD_DIR/$JAR_NAME" problem1.Problem1c_Stripes_IMC_Local \
    "$INPUT" \
    "$OUTPUT" \
    "$TOP50" \
    "$DIST"

END=$(date +%s)

# -------- OUTPUT --------
echo "======================================="
echo "Sample Output:"
echo "======================================="

cat "$OUTPUT/part-r-00000" | head -20

echo "======================================="
echo "Runtime: $((END - START)) seconds"
echo "======================================="
echo "DONE"