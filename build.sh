#!/bin/bash
# build.sh — Compiles the Maven project and produces problem2.jar in the project root.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

echo "=== Building problem2 (2a + 2b) with Maven ==="
cd "$SCRIPT_DIR"
mvn clean package -q

JAR_FILE="$(find "$SCRIPT_DIR/target" -maxdepth 1 -type f -name '*.jar' | grep -v '\-sources\.jar\|\-javadoc\.jar' | head -n 1)"
if [ -z "$JAR_FILE" ]; then
    echo "ERROR: No jar produced under $SCRIPT_DIR/target."
    exit 1
fi

cp "$JAR_FILE" "$SCRIPT_DIR/problem2.jar"

if [ -f "$SCRIPT_DIR/problem2.jar" ]; then
    echo ""
    echo "Build successful → $SCRIPT_DIR/problem2.jar"
    echo "  Entry points:"
    echo "    parta.DocumentFrequency  (Problem 2a — Document Frequency)"
    echo "    partb.TFIDFScorer        (Problem 2b — TF-IDF Scorer)"
else
    echo "ERROR: failed to copy jar to $SCRIPT_DIR/problem2.jar."
    exit 1
fi
