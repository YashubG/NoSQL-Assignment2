#!/bin/bash

set -e

INPUT_DIR="Data/full/Wikipedia-EN-20120601_ARTICLES"
OUTPUT_FILE="Data/wiki_all_fixed.txt"
TEMP_FILE="Data/_temp_merge.txt"

echo "======================================="
echo "Creating clean dataset (sentence-based)"
echo "======================================="

# -------- CLEAN --------
rm -f "$OUTPUT_FILE" "$TEMP_FILE"

# -------- MERGE FILES --------
echo "[1] Merging files..."

for file in "$INPUT_DIR"/*; do
    cat "$file" >> "$TEMP_FILE"
    echo " " >> "$TEMP_FILE"   # ensure separation
done

# -------- CLEAN + SPLIT SENTENCES --------
echo "[2] Splitting into sentences..."

sed -E '
    s/&quot;/ /g;
    s/&apos;/ /g;
    s/&amp;/ /g;
    s/http[^ ]+//g;
    s/www\.[^ ]+//g;
    s/[.!?]+/\n/g
' "$TEMP_FILE" > "$OUTPUT_FILE"

# -------- REMOVE EXTRA WHITESPACE --------
echo "[3] Cleaning whitespace..."

sed -i -E '
    s/^[ \t]+//;
    s/[ \t]+$//;
    /^$/d
' "$OUTPUT_FILE"

# -------- CLEAN TEMP --------
rm -f "$TEMP_FILE"

echo "======================================="
echo "Dataset created successfully"
echo "Output: $OUTPUT_FILE"
echo "======================================="