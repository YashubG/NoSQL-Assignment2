#!/bin/bash

set -e

echo "======================================="
echo "Running ALL Experiments (Parallel Mode)"
echo "======================================="

PROJECT_ROOT="$(pwd)"
OUTPUT_DIR="$PROJECT_ROOT/output/parts b,c,e"
LOG_FILE="$OUTPUT_DIR/runtime_log.txt"

mkdir -p "$OUTPUT_DIR"
rm -f "$LOG_FILE"

# ----------------------------
# CONFIG
# ----------------------------
DISTANCES=(1 2 3 4)
MAX_PARALLEL=2   # 🔥 change to 3 if stable

# ----------------------------
# FUNCTION TO RUN JOB
# ----------------------------
run_job () {
    SCRIPT=$1
    DIST=$2
    LABEL=$3

    echo "Running $LABEL (d=$DIST)..."

    START=$(date +%s)

    bash "$SCRIPT" "$DIST" > "$OUTPUT_DIR/${LABEL}_d${DIST}.txt" 2>&1

    END=$(date +%s)
    RUNTIME=$((END - START))

    echo "$LABEL d=$DIST : $RUNTIME sec" | tee -a "$LOG_FILE"
}

export -f run_job
export OUTPUT_DIR LOG_FILE

# ----------------------------
# JOB LIST
# ----------------------------

run_all () {
    for d in "${DISTANCES[@]}"; do
        run_job scripts/run_problem1b.sh $d "pairs(1b)" &
        run_job scripts/run_problem1c.sh $d "stripes(1c)" &
        wait

        run_job scripts/run_problem1b_imc.sh $d "pairs_imc(1b_imc)" &
        run_job scripts/run_problem1c_imc.sh $d "stripes_imc(1c_imc)" &
        wait
    done
}

# ----------------------------
# EXECUTE
# ----------------------------

run_all

echo "======================================="
echo "ALL JOBS COMPLETED"
echo "======================================="

echo "Runtime summary:"
cat "$LOG_FILE"