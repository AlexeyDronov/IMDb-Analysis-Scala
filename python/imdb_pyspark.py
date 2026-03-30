from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

DATA_DIR = "src/main/resources/imdb"


def create_session():
    return (
        SparkSession.builder
        .appName("ImdbAnalysis")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load_data(spark):
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
    )

    title_crew = (
        spark.read.csv(f"{DATA_DIR}/title.crew.tsv", **opts)
        .select("tconst", "directors", "writers")
    )

    name_basics = (
        spark.read.csv(f"{DATA_DIR}/name.basics.tsv", **opts)
        .filter(F.col("primaryName").isNotNull())
        .select("nconst", "primaryName")
        .cache()
    )

    return title_basics, title_ratings, title_crew, name_basics


def timed(label, fn):
    start = time.time()
    result = fn()
    print(f"Processing {label} took {round((time.time() - start) * 1000)} ms.")
    return result


# Task 1: Genre Growth Analysis
# Top 5 genres by title count growth between 1990–2000 and 2010–2020.
# Only movies and TV series.
def task1(title_basics):
    decade = F.when(F.col("startYear") <= 2000, "9").otherwise("1")

    genres = (
        title_basics
        .filter(
            F.col("titleType").isin("movie", "tvSeries")
            & F.col("genres").isNotNull()
            & (F.col("startYear").between(1990, 2000) | F.col("startYear").between(2010, 2020))
        )
        .withColumn("decade", decade)
        .withColumn("genre", F.explode(F.split("genres", ",")))
        .dropDuplicates(["tconst", "genre", "decade"])
    )

    return (
        genres
        .groupBy("genre").pivot("decade", ["9", "1"]).count().fillna(0)
        .withColumn("growth", F.col("1") - F.col("9"))
        .select("genre", "growth")
        .orderBy(F.col("growth").desc())
        .limit(5)
        .collect()
    )


# Task 2: Top 3 genres per decade by weighted average rating.
# Excludes genres with fewer than 5 titles in that decade.
def task2(title_basics, title_ratings):
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
def task3(name_basics, title_ratings, title_crew):
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
def task4(title_basics, name_basics, title_crew):
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
    spark = create_session()
    spark.sparkContext.setLogLevel("ERROR")

    title_basics, title_ratings, title_crew, name_basics = load_data(spark)

    r1 = timed("Task 1", lambda: task1(title_basics))
    print(r1)

    r2 = timed("Task 2", lambda: task2(title_basics, title_ratings))
    print(r2)

    r3 = timed("Task 3", lambda: task3(name_basics, title_ratings, title_crew))
    print(r3)

    r4 = timed("Task 4", lambda: task4(title_basics, name_basics, title_crew))
    print(r4)

    spark.stop()


if __name__ == "__main__":
    main()