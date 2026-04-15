#!/bin/bash

set -e

echo "======================================="
echo "Running Problem 1(a): Local Mode"
echo "======================================="

PROJECT_ROOT="$(pwd)"

SRC_DIR="$PROJECT_ROOT/src"
BUILD_DIR="$PROJECT_ROOT/build"
JAR_NAME="problem1a_local.jar"

INPUT="$PROJECT_ROOT/Data/wiki_all_fixed.txt"
STOPWORDS="$PROJECT_ROOT/Data/stopwords.txt"
OUTPUT="$PROJECT_ROOT/output/problem1a_local"

# -------- CLEAN --------
echo "[1] Cleaning..."
rm -rf "$BUILD_DIR" "$OUTPUT"
mkdir -p "$BUILD_DIR"

# -------- COMPILE --------
echo "[2] Compiling..."

javac -classpath "$(hadoop classpath)" -d "$BUILD_DIR" \
    "$SRC_DIR/problem1/Problem1a_TopWords_Local.java"

# -------- CREATE JAR --------
echo "[3] Creating JAR..."
cd "$BUILD_DIR"
jar -cvf "$JAR_NAME" .
cd "$PROJECT_ROOT"

# -------- RUN --------
echo "[4] Running job..."

START=$(date +%s)

hadoop jar "$BUILD_DIR/$JAR_NAME" problem1.Problem1a_TopWords_Local \
    "$INPUT" \
    "$OUTPUT" \
    "$STOPWORDS"

END=$(date +%s)

# -------- OUTPUT --------
echo "======================================="
echo "Top 50 Words:"
echo "======================================="

sort -k2 -nr "$OUTPUT/part-r-00000" | head -50

echo "======================================="
echo "Runtime: $((END - START)) seconds"
echo "======================================="
echo "DONE"