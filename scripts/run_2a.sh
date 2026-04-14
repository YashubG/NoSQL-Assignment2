#!/bin/bash
# run_2a.sh — Runs Problem 2a: Document Frequency on the full Wikipedia dataset (HDFS).
#
# Pre-requisites:
#   - Hadoop must be running and accessible via `hadoop` and `hdfs` commands.
#   - The Wikipedia dataset must be uploaded to HDFS at:
#       /user/$USER/input/wiki_full/Wikipedia-EN-20120601_ARTICLES
#   - The stopwords.txt file must be uploaded to HDFS at:
#       /user/$USER/stopwords.txt
#   - Java 11 and Maven must be installed.
#   - problem2.jar must exist (run ./build.sh first).
#
# Input  (HDFS): /user/$USER/input/wiki_full/Wikipedia-EN-20120601_ARTICLES
#                /user/$USER/stopwords.txt
# Output (local): <project-dir>/output/df_output.tsv
#                  <project-dir>/output/df_top100.tsv
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$SCRIPT_DIR/problem2.jar"
LOCAL_DF_OUTPUT="$SCRIPT_DIR/output/df_output.tsv"
LOCAL_TOP100_OUTPUT="$SCRIPT_DIR/output/df_top100.tsv"
HDFS_INPUT=/user/$USER/input/wiki_full/Wikipedia-EN-20120601_ARTICLES
HDFS_DF_OUTPUT=/user/$USER/output/friend_df_output
HDFS_TOP100_OUTPUT=/user/$USER/output/friend_df_top100
HDFS_STOPWORDS=/user/$USER/stopwords.txt

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/mnt/newstorage/hadoop
export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

mkdir -p "$SCRIPT_DIR/output"

# ── Pre-flight checks ──────────────────────────────────────────────

if [ ! -f "$JAR" ]; then
    echo "ERROR: $JAR not found. Run ./build.sh first."
    exit 1
fi

if ! hdfs dfs -test -e "$HDFS_INPUT" 2>/dev/null; then
    echo "ERROR: $HDFS_INPUT not on HDFS."
    echo "Upload it with:  hdfs dfs -put <local-path>/Wikipedia-EN-20120601_ARTICLES $HDFS_INPUT"
    exit 1
fi

if ! hdfs dfs -test -e "$HDFS_STOPWORDS" 2>/dev/null; then
    echo "ERROR: $HDFS_STOPWORDS not on HDFS."
    echo "Upload it with:  hdfs dfs -put stopwords.txt $HDFS_STOPWORDS"
    exit 1
fi

# ── Clean old outputs ──────────────────────────────────────────────

hdfs dfs -rm -r "$HDFS_DF_OUTPUT" 2>/dev/null && echo "Removed old HDFS DF output" || true
hdfs dfs -rm -r "$HDFS_TOP100_OUTPUT" 2>/dev/null && echo "Removed old HDFS top-100 output" || true

# ── Run Problem 2a ─────────────────────────────────────────────────

echo ""
echo "=== [2a] Running Document Frequency (Problem2a_DF) ==="
echo "Input       : $HDFS_INPUT"
echo "Stopwords   : $HDFS_STOPWORDS"
echo "DF Output   : $HDFS_DF_OUTPUT"
echo "Top100 Out  : $HDFS_TOP100_OUTPUT"
echo ""

START=$(date +%s)

hadoop jar "$JAR" parta.DocumentFrequency \
    "$HDFS_INPUT" \
    "$HDFS_DF_OUTPUT" \
    "$HDFS_TOP100_OUTPUT" \
    "$HDFS_STOPWORDS"

END=$(date +%s)

# ── Download results to local ──────────────────────────────────────

TEMP_DF="/tmp/friend_df_output_$$.tsv"
TEMP_TOP100="/tmp/friend_df_top100_$$.tsv"

hdfs dfs -getmerge "$HDFS_DF_OUTPUT" "$TEMP_DF"
mv "$TEMP_DF" "$LOCAL_DF_OUTPUT"

hdfs dfs -getmerge "$HDFS_TOP100_OUTPUT" "$TEMP_TOP100"
mv "$TEMP_TOP100" "$LOCAL_TOP100_OUTPUT"

# ── Upload top-100 to HDFS for Problem 2b ──────────────────────────

HDFS_TOP100_FOR_2B=/user/$USER/friend_df_top100.tsv
hdfs dfs -rm -f "$HDFS_TOP100_FOR_2B" 2>/dev/null || true
hdfs dfs -put "$LOCAL_TOP100_OUTPUT" "$HDFS_TOP100_FOR_2B"

echo ""
echo "=== [2a] Done in $((END-START))s ==="
echo ""
echo "Local outputs:"
echo "  DF (all terms) : $LOCAL_DF_OUTPUT"
echo "  Top 100 by DF  : $LOCAL_TOP100_OUTPUT"
echo "  Total terms    : $(wc -l < "$LOCAL_DF_OUTPUT")"
echo ""
echo "HDFS (for 2b):"
echo "  $HDFS_TOP100_FOR_2B"
echo ""
echo "Sample (top 10 by DF):"
echo "-------------------------------------"
head -10 "$LOCAL_TOP100_OUTPUT"
echo ""
echo "Next: run  bash scripts/run_2b.sh"
