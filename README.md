# NoSQL Assignment 2 — MapReduce Text Analytics on Wikipedia

> **Course:** NoSQL Databases  
> **Framework:** Apache Hadoop MapReduce (Java)  
> **Dataset:** Wikipedia English Articles (50-article sample + 10,000-article full corpus)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Problem 1 — Word Co-occurrence Analysis](#problem-1--word-co-occurrence-analysis)
  - [1(a) — Top 50 Words](#1a--top-50-words)
  - [1(b) — Pairs Approach](#1b--pairs-approach)
  - [1(c) — Stripes Approach](#1c--stripes-approach)
  - [1(d) — Comparison & Analysis](#1d--comparison--analysis)
  - [1(e) — Optimized Co-occurrence](#1e--optimized-co-occurrence)
- [Problem 2 — TF-IDF Computation](#problem-2--tf-idf-computation)
  - [2(a) — Document Frequency (DF)](#2a--document-frequency-df)
  - [2(b) — TF-IDF Scoring](#2b--tf-idf-scoring)
- [How to Build & Run](#how-to-build--run)
- [Output Files](#output-files)
- [Runtime Performance](#runtime-performance)

---

## Overview

This project implements several **MapReduce** algorithms in Java on **Apache Hadoop** to perform large-scale text analytics over the English Wikipedia corpus. The assignment is split into two main problems:

1. **Problem 1** — Building word co-occurrence matrices using **Pairs** and **Stripes** design patterns, along with in-mapper combining optimizations. The top-50 most frequent non-stopwords are identified first, and then co-occurrence matrices are computed for these words.

2. **Problem 2** — Computing **Document Frequency (DF)** and **TF-IDF scores** across the full 10,000-article Wikipedia corpus. This involves tokenization, Porter stemming, stopword removal, and a two-job MapReduce pipeline.

---

## Project Structure

```
.
├── src/
│   ├── problem1/
│   │   ├── Problem1a_TopWords.java        # Word frequency counter (top-50)
│   │   ├── Problem1b_Pairs.java           # Co-occurrence via Pairs pattern
│   │   ├── Problem1c_Stripes.java         # Co-occurrence via Stripes pattern
│   │   └── Problem1e_Optimized.java       # Optimized co-occurrence
│   ├── problem2/
│   │   ├── Problem2a_DF.java              # Document Frequency + Top-100 DF
│   │   └── Problem2b_TFScore.java         # (placeholder for TF scoring)
│   ├── inputformat/
│   │   ├── CustomFileInputFormat.java     # Custom InputFormat (whole-file-as-document)
│   │   └── CustomLineRecordReader.java    # Custom RecordReader
│   └── main/java/problem2/
│       └── TFIDFScorer.java               # TF-IDF scorer (Stripes + CombineTextInputFormat)
├── scripts/
│   ├── run_problem1a.sh                   # Run Problem 1(a) on HDFS
│   ├── run_problem2.sh                    # Run Problem 2(a) locally/HDFS
│   ├── run_2a.sh                          # Run Problem 2(a) on HDFS (full pipeline)
│   ├── run_2b.sh                          # Run Problem 2(b) on HDFS
│   └── extract_top100.sh                  # Extract top-100 DF terms
├── build/                                 # Compiled .class files and JARs
├── lib/
│   ├── opennlp-tools-1.9.3.jar           # OpenNLP for Porter Stemmer
│   └── opennlp-en-ud-ewt-pos-1.0-1.9.3.bin
├── output/                                # All MapReduce output results
├── stopwords.txt                          # Stopwords list (704 words)
├── build.sh                               # Build script (Maven)
├── pom.xml                                # Maven project configuration
├── problem2.jar                           # Pre-built JAR for Problem 2
└── Assignment-2.pdf                       # Assignment specification
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Java JDK | 11 or 17 |
| Apache Hadoop | 3.3.x |
| Apache Maven | 3.x (for building Problem 2) |
| OpenNLP | 1.9.3 (included in `lib/`) |

---

## Problem 1 — Word Co-occurrence Analysis

### 1(a) — Top 50 Words

**File:** `src/problem1/Problem1a_TopWords.java`

Identifies the **top 50 most frequently occurring words** in the Wikipedia corpus after removing stopwords.

**How it works:**
- **Mapper (`TokenizerMapper`):** Reads each line, converts to lowercase, removes all non-alphabetic characters, tokenizes, filters out stopwords (loaded via Hadoop Distributed Cache), and emits `(word, 1)` pairs.
- **Reducer (`IntSumReducer`):** Sums up counts for each word and outputs `(word, total_count)`.
- The final output is sorted by frequency to identify the top 50 words.

**Usage:**
```bash
hadoop jar build/problem1a.jar problem1.Problem1a_TopWords <input> <output> <stopwords>
```

**Sample output (top 5):**
```
aaa       90
aaaa       6
aaaaa      5
...
```
> The full sorted top-50 list is obtained by piping through `sort -k2 -nr | head -50`.

---

### 1(b) — Pairs Approach

**File:** `src/problem1/Problem1b_Pairs.java`

Constructs the **word co-occurrence matrix** using the **Pairs** design pattern.

**How it works:**
- For each document, a sliding window scans the text. For every pair of co-occurring words `(w_i, w_j)` within the window, the mapper emits the pair `((w_i, w_j), 1)`.
- The reducer aggregates these counts to produce the final co-occurrence count for each pair.
- An **In-Mapper Combining (IMC)** variant aggregates counts in a hash map within the mapper before emitting, significantly reducing shuffle/sort overhead.

**Output format:** `word_i,word_j → count`

**Sample output:**
```
age,age         696
age,b            30
age,barbarian     3
age,c            40
age,category     96
```

Each dataset (d1–d4) is run separately to analyze scalability. Outputs are stored in:
- `output/problem1b_local_d1/` through `d4/` — standard Pairs
- `output/problem1b_imc_local_d1/` through `d4/` — Pairs with In-Mapper Combining

---

### 1(c) — Stripes Approach

**File:** `src/problem1/Problem1c_Stripes.java`

Constructs the **word co-occurrence matrix** using the **Stripes** design pattern.

**How it works:**
- For each word `w_i`, the mapper builds an associative array (stripe) `{w_j: count, w_k: count, ...}` of all co-occurring words within the window.
- The reducer merges all stripes for the same key word by element-wise addition.
- An **In-Mapper Combining (IMC)** variant pre-aggregates stripes within the mapper.

**Output format:** `word → {co-word1=count, co-word2=count, ...}`

**Sample output:**
```
age       {population=167, party=23, time=196, first=288, game=45, ...}
barbarian {time=10, first=9, name=5, barbarian=26, city=2, ...}
category  {population=23, party=741, time=160, first=273, game=265, ...}
```

Outputs are stored in:
- `output/problem1c_local_d1/` through `d4/` — standard Stripes
- `output/problem1c_imc_local_d1/` through `d4/` — Stripes with In-Mapper Combining

---

### 1(d) — Comparison & Analysis

Performance comparison of **Pairs vs. Stripes** (with and without In-Mapper Combining) across four dataset splits:

| Algorithm | d=1 | d=2 | d=3 | d=4 |
|-----------|-----|-----|-----|-----|
| **Pairs (1b)** | 23s | 23s | 23s | 23s |
| **Stripes (1c)** | 32s | 32s | 31s | 32s |
| **Pairs + IMC (1b_imc)** | 29s | 28s | 29s | 29s |
| **Stripes + IMC (1c_imc)** | 29s | 29s | 30s | 29s |

**Key observations:**
- **Pairs** is consistently the fastest at ~23s, likely due to simpler serialization per record.
- **Stripes** takes ~32s due to the overhead of constructing and serializing associative arrays.
- **In-Mapper Combining** brings Stripes down from 32s → 29s (~10% improvement) by reducing the number of key-value pairs shuffled.
- Pairs + IMC is slightly slower than vanilla Pairs (29s vs 23s), suggesting the overhead of maintaining the in-mapper hash map outweighs the shuffle savings for the Pairs pattern on this dataset size.

Consolidated output files are also available in `output/parts b,c,e/`.

---

### 1(e) — Optimized Co-occurrence

**File:** `src/problem1/Problem1e_Optimized.java`

An optimized version of the co-occurrence computation combining the best strategies from 1(b)–1(d).

---

## Problem 2 — TF-IDF Computation

### 2(a) — Document Frequency (DF)

**File:** `src/problem2/Problem2a_DF.java`

Computes the **Document Frequency (DF)** for every term across the full 10,000-article Wikipedia corpus, then extracts the **top 100 terms by DF**.

**How it works (Two-Job Pipeline):**

**Job 1 — Document Frequency Calculation:**
- **Mapper (`DocumentFrequencyMapper`):**
  - Loads stopwords from the Distributed Cache.
  - Tokenizes each document, applies **Porter stemming** (via OpenNLP), and filters stopwords.
  - Tracks unique terms per document using a `HashSet` to avoid duplicate counting.
  - Emits `(stemmed_term, document_id)` for each unique term.
- **Reducer (`DocumentFrequencyReducer`):**
  - Collects all document IDs per term into a `HashSet` to count distinct documents.
  - Outputs `(term, DF_count)`.

**Job 2 — Top 100 Extraction:**
- **Mapper (`TopTermsMapper`):** Maintains a min-heap (`PriorityQueue`) of size 100, keeping only the top-100 highest DF terms.
- **Reducer (`TopTermsReducer`):** Merges heaps from all mappers and outputs the final sorted top 100.

**Custom InputFormat:** Uses `CustomFileInputFormat` which treats each file as a single document/record, ensuring correct per-document DF counting.

**Usage:**
```bash
# Sample dataset (50 articles)
./scripts/run_problem2.sh sample

# Full dataset (10,000 articles)
./scripts/run_problem2.sh full
```

**Sample output (top 10 by DF, from full 10,000-article corpus):**
```
s           9864
apo         9518
categori    9381
refer       9340
quot        8674
other       8415
link        8296
thi         8190
us          8063
extern      8008
```

Output files:
- `output/problem2a_df_full.tsv` — Full DF listing (all terms)
- `output/problem2a_top100_full.tsv` — Top 100 terms by DF
- `output/problem2a_df_sample/` — DF output (sample dataset)
- `output/problem2a_top100_sample/` — Top 100 output (sample dataset)

---

### 2(b) — TF-IDF Scoring

**File:** `src/main/java/problem2/TFIDFScorer.java`

Computes **TF-IDF scores** for the top-100 DF terms across all 10,000 Wikipedia articles.

**How it works:**
- **Mapper (`StripeMapper`):**
  - Loads the top-100 DF terms from the Distributed Cache.
  - Uses an **in-mapper combining stripes** approach: builds a `Map<docId, Map<term, count>>` entirely in memory.
  - Only counts terms that appear in the top-100 DF list.
  - Applies Porter stemming and validates tokens (minimum 3 chars, alphabetic only, no Roman numerals).
  - On `cleanup()`, emits the accumulated stripes.
- **Reducer (`TFIDFReducer`):**
  - Merges stripes from all mappers for each document.
  - Computes TF-IDF as: **`score = TF × log(N / DF + 1)`** where `N = 10,000` (total documents).
  - Outputs `(document_id, term, score)` in TSV format.

**Optimization:** Uses `CombineTextInputFormat` with 64 MB split size to merge 10,000 small files into ~15–20 map tasks, dramatically reducing task scheduling overhead.

**Usage:**
```bash
./scripts/run_2b.sh
```

**Output format:** `ID<tab>TERM<tab>SCORE`

---

## How to Build & Run

### Build Problem 2 (Maven)
```bash
./build.sh
```
This compiles the Maven project and produces `problem2.jar` in the project root.

### Build Problem 1 (Manual)
```bash
# Problem 1(a)
javac -classpath "$(hadoop classpath)" -d build/ \
    src/inputformat/*.java \
    src/problem1/Problem1a_TopWords.java
cd build && jar -cvf problem1a.jar . && cd ..
```

### Run Problem 1 on Hadoop (Local Mode)
```bash
# Step 1: Merge dataset files
bash scripts/DataMerger.sh

# Step 2: Problem 1(a) — Top 50 Words
bash scripts/run_problem1a.sh

# Step 3: All co-occurrence experiments (Pairs, Stripes, IMC variants)
bash scripts/run_all_experiments_in_prob1.sh
```
Results are stored in `output/`.

### Run Problem 2a — Document Frequency (HDFS)
```bash
# Step 1: Build the JAR
bash build.sh

# Step 2: Run Problem 2a (DF + Top-100 extraction)
bash scripts/run_2a.sh
```
This runs a two-job MapReduce pipeline on HDFS:
- **Job 1** — Computes Document Frequency (DF) for every stemmed term across all 10,000 articles.
- **Job 2** — Extracts the top 100 terms by DF using a min-heap.

Output:
- `output/df_output.tsv` — Full DF listing (all terms)
- `output/df_top100.tsv` — Top 100 terms by document frequency

The top-100 file is also automatically uploaded to HDFS at `/user/$USER/friend_df_top100.tsv` for use by Problem 2b.

> **Note:** For local-mode runs (without HDFS), you can also use:
> ```bash
> bash scripts/run_problem2.sh sample   # 50 articles
> bash scripts/run_problem2.sh full     # 10,000 articles
> ```

### Run Problem 2b — TF-IDF Scoring (HDFS)
```bash
# Step 3: Run Problem 2b (requires 2a output on HDFS)
bash scripts/run_2b.sh
```
Output: `output/tfidf_output.tsv`

---

## Output Files

All results are in the `output/` directory:

| Path | Description |
|------|-------------|
| `output/problem1a_local/` | Problem 1(a) — word frequencies |
| `output/problem1b_local_d1/` – `d4/` | Problem 1(b) — Pairs co-occurrence |
| `output/problem1b_imc_local_d1/` – `d4/` | Problem 1(b) — Pairs + In-Mapper Combining |
| `output/problem1c_local_d1/` – `d4/` | Problem 1(c) — Stripes co-occurrence |
| `output/problem1c_imc_local_d1/` – `d4/` | Problem 1(c) — Stripes + In-Mapper Combining |
| `output/parts b,c,e/` | Consolidated output text files + runtime log |
| `output/problem2a_df_full.tsv` | Full document frequency listing |
| `output/problem2a_top100_full.tsv` | Top 100 terms by document frequency |
| `output/problem2a_df_sample/` | DF output (sample dataset) |
| `output/problem2a_top100_sample/` | Top 100 output (sample dataset) |

---

## Runtime Performance

**Problem 1 — Co-occurrence (50 Wikipedia articles):**

| Algorithm | Avg. Runtime |
|-----------|-------------|
| Pairs | ~23s |
| Stripes | ~32s |
| Pairs + IMC | ~29s |
| Stripes + IMC | ~29s |

**Problem 2 — TF-IDF (10,000 Wikipedia articles):**
- Uses `CombineTextInputFormat` (64 MB splits) to reduce map tasks from 10,000 → ~15–20.
- Porter stemming via OpenNLP for normalization.

---

## Tech Stack

- **Language:** Java 11/17
- **Framework:** Apache Hadoop 3.3.6 (MapReduce)
- **NLP:** Apache OpenNLP 1.9.3 (Porter Stemmer)
- **Build:** Apache Maven + shell scripts
- **Dataset:** Wikipedia English (50 + 10,000 articles)
