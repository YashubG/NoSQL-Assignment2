# 🚀 Hadoop MapReduce Assignment (NoSQL Systems)

## 📌 Overview

This project implements **MapReduce algorithms** for large-scale text processing using Hadoop (Local Mode).
It covers:

- 🔤 Top-K frequent words
- 🔗 Co-occurrence matrices (Pairs & Stripes)
- ⚡ Optimizations (Combiner + In-Mapper Combining)

---

## 🧠 Key Concepts

### 1️⃣ Top Words (WordCount)

- Remove stopwords
- Count word frequencies
- Extract **Top 50 words**

---

### 2️⃣ Co-occurrence Matrix

#### 🔹 Pairs Approach

```
(word_i, word_j) → count
```

- Simple but generates **large intermediate data**

#### 🔹 Stripes Approach

```
word_i → {word_j : count}
```

- More efficient (reduced emissions)

---

### 3️⃣ ⚡ Optimizations

- 🧩 **Combiner** → reduces shuffle data
- 🔥 **In-Mapper Combining (IMC)** → aggregates inside mapper

👉 IMC gives the **best performance**

---

## 🚨 Critical Learning (Very Important)

### ❌ Initial Issue

- Extremely slow execution (>1 hour)
- High CPU usage but low progress

### 🔍 Root Cause

- Hadoop processes **one line = one record**
- Input contained **very long lines**
- Entire dataset treated as **single record**

---

### ✅ Fix Applied

- Inserted newlines between files
- Limited line length (fixed-width splitting)
- Enabled proper input splitting

💥 Result:

```
Runtime reduced from >1 hour → ~20 seconds
```

---

## ⚙️ How to Run

### 1️⃣ Make scripts executable

```bash
chmod +x scripts/*.sh
```

---

### 2️⃣ Run Top Words

```bash
bash scripts/run_problem1a_local.sh
```

---

### 3️⃣ Run Co-occurrence (Pairs / Stripes)

```bash
bash scripts/run_problem1b_local.sh <d>
bash scripts/run_problem1c_local.sh <d>
```

---

### 4️⃣ Run Optimized Versions (IMC)

```bash
bash scripts/run_problem1b_imc_local.sh <d>
bash scripts/run_problem1c_imc_local.sh <d>
```

---

### 5️⃣ Run All Experiments (Parallel)

```bash
bash scripts/run_all_experiments.sh
```

---

## 📊 Results (IMC Versions)

| Method           | d=1 | d=2 | d=3 | d=4 |
| ---------------- | --- | --- | --- | --- |
| 🔗 Pairs + IMC   | 9s  | 9s  | 10s | 10s |
| 📦 Stripes + IMC | 9s  | 9s  | 9s  | 10s |

---

## 📈 Observations

- ⚡ IMC drastically reduces runtime
- 📦 Stripes slightly outperforms pairs
- 📉 Runtime increases with window size `d`
- 🧠 Data formatting has massive performance impact

---

## 🧾 Key Takeaways

- 📌 Data structure matters as much as algorithms
- ⚠️ Avoid long input records in Hadoop
- 🚀 Preprocessing can give **10–100× speedup**
- 🔥 IMC is the most effective optimization

---

## ✅ Final Status

✔ Fully working MapReduce pipeline
✔ Optimized for large datasets
✔ Stable and efficient execution

---

💡 _Lesson learned:_

> In big data systems, **how you format data can matter more than how you process it.**
