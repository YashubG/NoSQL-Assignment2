#!/bin/bash

echo "[0] Starting Hadoop services..."
start-dfs.sh || true
start-yarn.sh || true
sleep 3

set -e

echo "======================================="
echo "Running Problem 1(a): Top 50 Words"
echo "======================================="

# -------- PROJECT ROOT --------
PROJECT_ROOT="$(pwd)"

# -------- PATHS --------
SRC_DIR="$PROJECT_ROOT/src"
BUILD_DIR="$PROJECT_ROOT/build"
JAR_NAME="problem1a.jar"

INPUT_LOCAL="$PROJECT_ROOT/Data/Wikipedia-50-ARTICLES"
STOPWORDS_LOCAL="$PROJECT_ROOT/Data/stopwords.txt"

INPUT_HDFS="/wiki"
OUTPUT_HDFS="/output1"
STOPWORDS_HDFS="/stopwords.txt"

# -------- CLEAN BUILD --------
echo "[1] Cleaning build directory..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# -------- COMPILE --------
echo "[2] Compiling Java files..."

javac -classpath "$(hadoop classpath)" -d "$BUILD_DIR" \
    "$SRC_DIR/inputformat/"*.java \
    "$SRC_DIR/problem1/Problem1a_TopWords.java"

# -------- CREATE JAR --------
echo "[3] Creating JAR..."
cd "$BUILD_DIR"
jar -cvf "$JAR_NAME" .
cd "$PROJECT_ROOT"

# -------- CLEAN HDFS --------
echo "[4] Cleaning HDFS..."
hdfs dfs -rm -r -f "$INPUT_HDFS" || true
hdfs dfs -rm -r -f "$OUTPUT_HDFS" || true
hdfs dfs -rm -f "$STOPWORDS_HDFS" || true

# -------- UPLOAD DATA --------
echo "[5] Uploading dataset..."
hdfs dfs -mkdir -p "$INPUT_HDFS"
hdfs dfs -put "$INPUT_LOCAL"/* "$INPUT_HDFS"

# -------- UPLOAD STOPWORDS --------
echo "[6] Uploading stopwords..."
hdfs dfs -put "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

# -------- RUN JOB --------
echo "[7] Running Hadoop job..."

hadoop jar "$BUILD_DIR/$JAR_NAME" problem1.Problem1a_TopWords \
    "$INPUT_HDFS" "$OUTPUT_HDFS" "$STOPWORDS_HDFS"

# -------- DISPLAY OUTPUT --------
echo "======================================="
echo "Top 50 Words:"
echo "======================================="

hdfs dfs -cat "$OUTPUT_HDFS/part-r-00000" | sort -k2 -nr | head -50

echo "======================================="
echo "DONE"
echo "======================================="