# IMDb Data Analysis with Apache Spark

**Grade: 95%** (Original Scala Implementation)

## Overview

This project analyzes IMDb dataset using Apache Spark to extract insights about movie titles, ratings, genres, and crew members. The project consists of four core analytical tasks.

Originally implemented in **Scala** using the low-level **Spark RDD API**, the project was extended with a complete **PySpark** implementation using the **Spark SQL / DataFrame API**. The extension includes a custom benchmarking suite that compares the performance of the RDD API against the highly optimised DataFrame API under identical Spark configurations and logic.

## My Contributions

**Original Coursework (Scala + Spark RDD API – 95% Grade)**
Implemented a complete end-to-end analysis pipeline on IMDb data to deliver four core insights:
- Genre growth trends between 1990-2000 and 2010-2020
- Top genres by weighted average rating per decade
- Highest rated directors with minimum of 3 films (≥8.5 rating) and over 10,000 votes
- Most prolific crew members (directors & writers) across three time windows

Applied RDD best practices including early filtering, `reduceByKey`, `HashPartitioner`, and caching.

**PySpark Extension**
- Re-implemented the **exast same four tasks** on the full **2.28GB, 41M+ records** dataset instead of the original subset.
- Added a custom benchmarking suite, structured logging, and a head-to-head performance comparison between the RDD API and DataFrame APIs.
- Preserved identical Spark configurations and business logic in both implementations

The extension delivered a **76% average runtime reduction** (59s &rarr; 14s across 10 trials)


## Optimisations

To ensure maximum performance and fair benchmarking across both implementations, several optimisations were applied:

#### General Optimisations (Both Implementations)
- **Caching & Warm-ups**: Datasets are explicitly loaded and `.cache()`'d before starting the benchmark timer. A "warm-up" run eliminates skew from Spark's lazy evaluation, DAG generation, and JVM class loading.
- **Resource Configuration**: Both Spark sessions use identical settings: `12GB` driver memory, a default parallelism of `20`, and `KryoSerializer`.

#### Scala (RDD API) Optimisations
- **Data Partitioning**: Used `HashPartitioner` to minimise data shuffling across the network during `join` and `reduceByKey` operations.
-   **Efficient Aggregations:** Preferred `reduceByKey` over `groupByKey` for map-side reductions.
-   **Early Filtering:** Applied filters as early as possible in the transformation chain.

#### PySpark (SQL / DataFrame API) Optimisations
- **Adaptive Query Execution (AQE)**: Enabled with dynamic partition coalescing and skew join handling for runtime query plan optimisation.
- **Column Pruning**: Used early `.select()` and `.dropDuplicates()` to processes only required columns and rows.



## How to Run

### Prerequisites

- **Java**: Java 11
- **Scala**: 2.12.15 (Managed via SBT)
- **SBT**: 1.9.6
- **Python**: 3.8
- **Spark Version**: 3.0.3 (Defined in `build.sbt` and `requirements.txt`)

### 1. Environment Setup

**Linux / macOS:**

```bash
# Find your Java 11 path and export it to the current shell session
export JAVA_HOME=/path/to/your/java/11
export PATH=$JAVA_HOME/bin:$PATH

# Verify the version is correct
java --version
```

**Python Setup:**
```bash
cd python
python -m venv venv
source venv/bin/activate
pip install -r python/requirements.txt
```

### 2. Getting the data

1. Download the following files from [the official IMDb Non-Commercial datasets page](https://datasets.imdbws.com/):
    - `name.basics.tsv.gz`
    - `title.basics.tsv.gz`
    - `title.crew.tsv.gz`
    - `title.ratings.tsv.gz`
2. Unzip and place the `.tsv` files in an `imdb` folder under `src/main/resources/`

**Dataset Size:**
| File | Size | Number of lines |
| ---- | ---- | --------------- |
| `nameBasics` | 0.88 GB | 15,201,379 |
| `titleBasics` | 1 GB | 12,398,189 |
| `titleCrew` | 0.38 GB | 12,398,189 |
| `titleRatings` | 0.02 GB | 1,654,329 |
| **Total** | **2.28 GB** | **41,652,086** |

### 3. Running the Scripts
**Scala RDD**
```bash
sbt run                         # Single run 
sbt "runMain imdb.ImdbSpark 10" # 10-trial benchmark
```

**PySpark SQL**
```bash
python python/imdb_pyspark.py --verbose             # Sincle run
python python/imdb_pyspark.py --verbose --trials 10 # 10-trial benchmark
```
`--verbose` flag prints task results alongside timings 
`--trials` specifies number of benchmark trials (>=2 to get stats; default: 1 for single run)

## Benchmarking: Scala RDD vs. PySpark SQL
> [!Note]
> Benchmarks ran on Macbook Pro M4 (10 cores, 24Gb RAM) over **10 trials** each.


<table>
  <thead>
    <tr>
      <th rowspan="5">Task</th>
      <th colspan="3">Scala RDD</th>
      <th colspan="3">PySpark SQL</th>
    </tr>
    <tr>
      <th>Mean (s)</th>
      <th>Median (s)</th>
      <th>Std (s)</th>
      <th>Mean (s)</th>
      <th>Median (s)</th>
      <th>Std (s)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Task 1</td>
      <td>3.51</td>
      <td>3.50</td>
      <td>0.36</td>
      <td>0.20</td>
      <td>0.19</td>
      <td>0.01</td>
    </tr>
    <tr>
      <td>Task 2</td>
      <td>11.08</td>
      <td>10.85</td>
      <td>0.54</td>
      <td>4.17</td>
      <td>4.14</td>
      <td>0.17</td>
    </tr>
    <tr>
      <td>Task 3</td>
      <td>17.97</td>
      <td>18.10</td>
      <td>0.52</td>
      <td>1.86</td>
      <td>1.87</td>
      <td>0.15</td>
    </tr>
    <tr>
      <td>Task 4</td>
      <td>26.70</td>
      <td>26.26</td>
      <td>2.29</td>
      <td>8.18</td>
      <td>8.20</td>
      <td>0.23</td>
    </tr>
    <tr>
      <td><b>Total</b></td>
      <td><b>59.26</b></td>
      <td><b>58.72</b></td>
      <td><b>-</b></td>
      <td><b>14.41</b></td>
      <td><b>14.40</b></td>
      <td><b>-</b></td>
    </tr>
  </tbody>
</table>

<!-- 
## Scala Spark RDD API Results
Task 1: mean=3509.63 ms, median=3497.81 ms, std=362.33 ms.
Task 2: mean=11083.34 ms, median=10849.60 ms, std=542.59 ms.
Task 3: mean=17966.30 ms, median=18102.50 ms, std=521.28 ms.
Task 4: mean=26700.69 ms, median=26256.50 ms, std=2287.00 ms.

## PySpark SQL API Results
Task 1: mean=197.62 ms, median=191.96 ms, std=16.03 ms.
Task 2: mean=4169.95 ms, median=4140.11 ms, std=168.95 ms.
Task 3: mean=1864.03 ms, median=1879.23 ms, std=150.04 ms.
Task 4: mean=8176.73 ms, median=8203.43 ms, std=232.20 ms. 
-->

### Sample Task Outputs (PySpark Single Run)
**Task 2 – Top 3 Genres by Decade**
```plain-text
1900: [('Adventure', 7.70), ('Fantasy', 6.88), ('Action', 6.79)]
...
2020: [('Animation', 7.89), ('Sport', 7.51), ('Biography', 7.37)]
```

**Task 3 – Top 10 Directors**
```plain-text
Hiroshi Kôjina: 9.60
Hiroaki Miyamoto: 9.58
Naomi Nakayama: 9.57
...
```

### Why PySpark SQL Outperformed Scala RDD

The PySpark implementation achieved an overall **75.7% runtime reduction** compared to the original Scala RDD version. This performance gap is primarily explained by:

1. **The Catalyst Optimiser**: automatically analyses the logical plan, applies rule-based optimisations, and generates highly efficient bytecode.
2. **Tungsten Execution Engine**: provides cache-aware memory management, reduced garbage collection, and lower CPU overhead.