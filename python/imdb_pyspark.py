import os
import time
import argparse
from argparse import Namespace
from statistics import stdev, mean, median
from collections import defaultdict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
DATA_DIR = "src/main/resources/imdb"

# -- CLI ------------------------------------------------------------------------------------

def parse_cli_args() -> Namespace:    
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--trials",
                        type=int,
                        default=1,
                        metavar="N",
                        help="Number of benchmark trials (>=2 to get stats; default: 1 for single run)")
    parser.add_argument("-v", "--verbose",
                        action="store_true",
                        help="Print task results alongside timings")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("--trials must be >=1")
    return args

# -- Timing ---------------------------------------------------------------------------------

def timed(fn):
    start = time.perf_counter()
    result = fn()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return result, elapsed_ms

# -- Logging --------------------------------------------------------------------------------

def _format_result(result) -> str:
    if isinstance(result, list) and all(isinstance(item, tuple) for item in result):
        return "\n".join(f"    {k}: {v}" for k, v in result)
    if isinstance(result, list):
        return "\n".join(f"    {row}" for row in result)
    return f"    {result}"

def log_task_result(name, result, duration, verbose):
    print(f"{name}: {duration:.2f} ms")
    if verbose:
        print(_format_result(result))
        print("-" * 40)

# -- Run Modes ------------------------------------------------------------------------------

def run_once(tasks, verbose: bool) -> None:
    for name, fn in tasks:
        result, t = timed(fn)
        log_task_result(name, result, t, verbose)

def benchmark(tasks, trials: int, verbose: bool) -> None:
    if trials < 2:
        raise ValueError("benchmark requires trials >= 2 for meaningful stats")
    
    # Warm-up run due to Spark lazy transformations bias
    print("Warming up...")
    run_once(tasks, verbose)
    
    times: dict[str, list[float]] = defaultdict(list)
    for idx in range(trials):
        print(f"Running Trial {idx + 1}")
        for name, fn in tasks:
            _, t = timed(fn)
            times[name].append(t)

    print("\nBenchmark results:")
    for name, values in times.items():
        print(
            f"{name}: "
            f"mean={mean(values):.2f} ms, "
            f"median={median(values):.2f} ms, "
            f"std={stdev(values):.2f} ms."
        )


def create_session():
    return (
        SparkSession.builder
        .appName("ImdbAnalysis")
        .master("local[*]")
        .config("spark.driver.memory", "12g")

        .config("spark.sql.default.parallelism", "20")
        .config("spark.sql.shuffle.partitions", "20")
        
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "100MB")

        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_data(spark: SparkSession):
    opts = {"sep": "\t", "header": True, "nullValue": "\\N"}

    title_basics = (
        spark.read.csv(f"{DATA_DIR}/title.basics.tsv", **opts)
        .select(
            "tconst", "titleType", "genres",
            F.col("startYear").cast("int").alias("startYear"),
        )
        .cache()
    )

    title_ratings = (
        spark.read.csv(f"{DATA_DIR}/title.ratings.tsv", **opts)
        .select(
            "tconst",
            F.col("averageRating").cast("float").alias("averageRating"),
            F.col("numVotes").cast("int").alias("numVotes"),
        )
        .cache()
    )

    title_crew = (
        spark.read.csv(f"{DATA_DIR}/title.crew.tsv", **opts)
        .select("tconst", "directors", "writers")
        .cache()
    )

    name_basics = (
        spark.read.csv(f"{DATA_DIR}/name.basics.tsv", **opts)
        .filter(F.col("primaryName").isNotNull())
        .select("nconst", "primaryName")
        .cache()
    )

    return title_basics, title_ratings, title_crew, name_basics


# Task 1: Genre Growth Analysis
# Top 5 genres by title count growth between 1990–2000 and 2010–2020.
# Only movies and TV series.
def task1(title_basics: DataFrame):
    filtered = (
        title_basics
        .filter(
            (F.col("titleType").isin("movie", "tvSeries")) &
            (F.col("genres").isNotNull()) &
            (
                F.col("startYear").between(1990, 2000) |
                F.col("startYear").between(2010, 2020)
            )
        )
        .select("tconst", "genres", "startYear")  # drop unused cols early
    )

    genres = (
        filtered
        .withColumn(
            "decade",
            F.when(F.col("startYear") <= 2000, "9").otherwise("1")
        )
        .withColumn("genre", F.explode(F.split("genres", ",")))
    )

    result = (
        genres
        .groupBy("genre")
        .agg(
            F.sum(F.when(F.col("decade") == "9", 1).otherwise(0)).alias("d9"),
            F.sum(F.when(F.col("decade") == "1", 1).otherwise(0)).alias("d1"),
        )
        .withColumn("growth", F.col("d1") - F.col("d9"))
        .select("genre", "growth")
        .orderBy(F.col("growth").desc())
        .limit(5)
        .collect()
    )

    return result

# Task 2: Top 3 genres per decade by weighted average rating.
# Excludes genres with fewer than 5 titles in that decade.
def task2(title_basics: DataFrame, title_ratings: DataFrame):
    basics = (
        title_basics
        .filter(F.col("startYear").between(1900, 2029) & F.col("genres").isNotNull())
        .withColumn("decade", (F.col("startYear") - F.col("startYear") % 10).cast("int"))
        .withColumn("genre", F.explode(F.split("genres", ",")))
        .dropDuplicates(["tconst", "genre"])
    )

    agg = (
        basics.join(title_ratings, "tconst")
        .groupBy("decade", "genre")
        .agg(
            F.sum("numVotes").alias("total_votes"),
            F.sum(F.col("averageRating") * F.col("numVotes")).alias("weighted_sum"),
            F.count("tconst").alias("title_count"),
        )
        .filter(F.col("title_count") >= 5)
        .withColumn("weighted_avg", F.col("weighted_sum") / F.col("total_votes"))
    )

    window = Window.partitionBy("decade").orderBy(F.col("weighted_avg").desc())
    rows = (
        agg
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 3)
        .orderBy("decade", F.col("weighted_avg").desc())
        .select("decade", "genre", "weighted_avg")
        .collect()
    )

    result = {}
    for row in rows:
        result.setdefault(row.decade, []).append((row.genre, float(row.weighted_avg)))
    return sorted(result.items())


# Task 3: Top 10 directors by average rating.
# Must have at least 3 films with 10k+ votes and rating > 8.5.
def task3(name_basics: DataFrame, title_ratings: DataFrame, title_crew: DataFrame):
    qualifying = F.broadcast(
        title_ratings.filter(
            (F.col("numVotes") >= 10000) & (F.col("averageRating") > 8.5)
        )
    )

    directors = (
        title_crew
        .filter(F.col("directors").isNotNull())
        .withColumn("director", F.explode(F.split("directors", ",")))
    )

    stats = (
        directors.join(qualifying, "tconst")
        .groupBy("director")
        .agg(
            F.count("tconst").alias("film_count"),
            F.sum("averageRating").alias("total_rating"),
        )
        .filter(F.col("film_count") >= 3)
        .withColumn("avg_rating", F.col("total_rating") / F.col("film_count"))
    )

    return (
        stats.join(name_basics, stats.director == name_basics.nconst)
        .select("primaryName", "avg_rating")
        .orderBy(F.col("avg_rating").desc(), "primaryName")
        .limit(10)
        .collect()
    )


# Task 4: Top 10 most prolific directors/writers across 1995–2000, 2005–2010, 2015–2020.
def task4(title_basics: DataFrame, name_basics: DataFrame, title_crew: DataFrame):
    valid_titles = (
        title_basics
        .filter(
            F.col("startYear").between(1995, 2000)
            | F.col("startYear").between(2005, 2010)
            | F.col("startYear").between(2015, 2020)
        )
        .select("tconst")
    )

    crew = (
        title_crew
        .filter(F.col("directors").isNotNull() | F.col("writers").isNotNull())
        .withColumn("member", F.explode(
            F.split(F.concat_ws(",", "directors", "writers"), ",")
        ))
        .filter(F.col("member") != "")
        .dropDuplicates(["tconst", "member"])
    )

    counts = (
        crew.join(valid_titles, "tconst")
        .groupBy("member")
        .agg(F.count("tconst").alias("contributions"))
    )

    return (
        counts.join(name_basics, counts.member == name_basics.nconst)
        .select("primaryName", "contributions")
        .orderBy(F.col("contributions").desc(), "primaryName")
        .limit(10)
        .collect()
    )


def main():
    args = parse_cli_args()
    spark = create_session()
    spark.sparkContext.setLogLevel("FATAL")

    title_basics, title_ratings, title_crew, name_basics = load_data(spark)

    TASKS = [
        ("Task 1", lambda: task1(title_basics)),
        ("Task 2", lambda: task2(title_basics, title_ratings)),
        ("Task 3", lambda: task3(name_basics, title_ratings, title_crew)),
        ("Task 4", lambda: task4(title_basics, name_basics, title_crew))
    ]

    if args.trials == 1:
        run_once(TASKS, args.verbose)
    else:
        benchmark(TASKS, args.trials, args.verbose)

    spark.stop()


if __name__ == "__main__":
    main()