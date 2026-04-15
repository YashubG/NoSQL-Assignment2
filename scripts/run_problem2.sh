#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPENNLP_JAR="$ROOT_DIR/lib/opennlp-tools-1.9.3.jar"
STOPWORDS_FILE="${STOPWORDS_FILE:-$ROOT_DIR/Data/stopwords.txt}"
BUILD_DIR="$ROOT_DIR/build/classes"
JAR_FILE="$ROOT_DIR/build/problem2a-df.jar"
HADOOP_CMD="${HADOOP_CMD:-hadoop}"

MODE="${1:-sample}"
case "$MODE" in
	sample)
		ARCHIVE="${ARCHIVE:-/Users/agammanashroy/Downloads/Assignment 2/Wikipedia-50-ARTICLES.tar}"
		ARTICLE_DIR="$ROOT_DIR/Data/Wikipedia-50-ARTICLES"
		;;
	full)
		ARCHIVE="${ARCHIVE:-/Users/agammanashroy/Downloads/Assignment 2/Wikipedia-EN-20120601_ARTICLES.tar.gz}"
		ARTICLE_DIR="$ROOT_DIR/Data/Wikipedia-EN-20120601_ARTICLES"
		;;
	*)
		echo "Usage: $0 [sample|full]"
		echo "Optional environment variables: ARCHIVE=/path/to/archive STOPWORDS_FILE=/path/to/stopwords.txt"
		exit 2
		;;
esac

if ! command -v "$HADOOP_CMD" >/dev/null 2>&1; then
	if [ -x /opt/homebrew/opt/hadoop/bin/hadoop ]; then
		HADOOP_CMD=/opt/homebrew/opt/hadoop/bin/hadoop
	elif [ -x /usr/local/opt/hadoop/bin/hadoop ]; then
		HADOOP_CMD=/usr/local/opt/hadoop/bin/hadoop
	fi
fi

if ! command -v "$HADOOP_CMD" >/dev/null 2>&1; then
	echo "ERROR: hadoop is not on PATH. Install Hadoop or add its bin directory to PATH, then rerun this script."
	exit 1
fi

if [ ! -f "$OPENNLP_JAR" ]; then
	echo "ERROR: OpenNLP jar not found at $OPENNLP_JAR"
	exit 1
fi

if [ ! -f "$STOPWORDS_FILE" ]; then
	echo "ERROR: stopwords file not found at $STOPWORDS_FILE"
	exit 1
fi

if [ ! -d "$ARTICLE_DIR" ]; then
	if [ ! -f "$ARCHIVE" ]; then
		echo "ERROR: archive not found at $ARCHIVE"
		exit 1
	fi

	echo "Extracting $ARCHIVE into $ROOT_DIR/Data"
	mkdir -p "$ROOT_DIR/Data"
	tar -xf "$ARCHIVE" -C "$ROOT_DIR/Data"
fi

DF_OUTPUT="$ROOT_DIR/output/problem2a_df_$MODE"
TOP100_OUTPUT="$ROOT_DIR/output/problem2a_top100_$MODE"

rm -rf "$BUILD_DIR" "$JAR_FILE" "$DF_OUTPUT" "$TOP100_OUTPUT"
mkdir -p "$BUILD_DIR" "$(dirname "$JAR_FILE")" "$ROOT_DIR/output"

HADOOP_CP="$("$HADOOP_CMD" classpath)"
javac --release 17 -cp "$HADOOP_CP:$OPENNLP_JAR" -d "$BUILD_DIR" \
	"$ROOT_DIR/src/inputformat/CustomFileInputFormat.java" \
	"$ROOT_DIR/src/inputformat/CustomLineRecordReader.java" \
	"$ROOT_DIR/src/problem2/Problem2a_DF.java"
# Bundle OpenNLP classes into the jar (spaces in path break HADOOP_CLASSPATH)
(cd "$BUILD_DIR" && jar xf "$OPENNLP_JAR")
jar cf "$JAR_FILE" -C "$BUILD_DIR" .

"$HADOOP_CMD" jar "$JAR_FILE" src.problem2.Problem2a_DF \
	"$ARTICLE_DIR" \
	"$DF_OUTPUT" \
	"$TOP100_OUTPUT" \
	"$STOPWORDS_FILE"

echo
echo "Document-frequency TSV:"
echo "$DF_OUTPUT/part-r-00000"
echo
echo "Top 100 DF terms TSV:"
echo "$TOP100_OUTPUT/part-r-00000"
