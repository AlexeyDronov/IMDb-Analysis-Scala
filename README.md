# IMDb Data Analysis with Apache Spark

**Grade: 95%**

## Overview

This project analyzes a subset of the IMDb dataset using Apache Spark and Scala. The goal is to extract insights from the data by performing various analyses on movie titles, ratings, genres, and crew members. The project is divided into four main tasks, each addressing a specific question about the dataset.

The project is implemented using Apache Spark's RDD API to process and analyze the data in a distributed manner. The code is written in Scala and uses sbt as the build tool.

## My Contributions

I have implemented all four tasks of the project, which involve analyzing genre growth, genre ratings by decade, top directors, and prolific crew members. I have also implemented several optimizations to improve the performance of the Spark jobs.

### [Task 1: Genre Growth Analysis](src/main/scala/ImdbSpark.scala#L58)

This task finds the top 5 genres that have experienced the most growth in popularity between the 1990-2000 and 2010-2020 periods. The implementation involves:

1.  Filtering the dataset to include only movies and TV shows from the specified periods.
2.  Extracting the genres and the decade for each title.
3.  Counting the number of titles for each genre in each decade.
4.  Calculating the growth in the number of titles for each genre between the two decades.
5.  Sorting the genres by their growth in descending order and taking the top 5.

### [Task 2: Genre Ratings by Decade](src/main/scala/ImdbSpark.scala#L98)

This task identifies the top 3 genres for each decade based on their weighted average ratings. The implementation involves:

1.  Filtering the dataset to include only titles with a valid start year and genres.
2.  Calculating the weighted average rating for each genre in each decade.
3.  Filtering out genres with fewer than 5 entries in each decade.
4.  Sorting the genres by their weighted average rating in descending order for each decade and taking the top 3.

### [Task 3: Top Directors](src/main/scala/ImdbSpark.scala#L154)

This task finds the directors with the highest average ratings for their films. The implementation involves:

1.  Filtering for directors who have at least 3 films rated above 8.5 with 10,000+ votes.
2.  Calculating the average rating for each of these directors.
3.  Joining the results with the names of the directors.
4.  Sorting the directors by their average rating in descending order and taking the top 10.

### [Task 4: Prolific Crew Members](src/main/scala/ImdbSpark.scala#L206)

This task identifies the top 10 most prolific crew members (directors and writers) across specific time periods (1995-2000, 2005-2010, and 2015-2020). The implementation involves:

1.  Filtering the titles to include only those from the specified time periods.
2.  Extracting the crew members (directors and writers) for each title.
3.  Counting the number of unique contributions for each crew member.
4.  Joining the results with the names of the crew members.
5.  Sorting the crew members by their total number of unique contributions in descending order and taking the top 10.

## Optimizations

Several optimizations have been implemented to improve the performance of the Spark jobs:

*   **Data Partitioning:** In all tasks, RDDs are partitioned using a `HashPartitioner`. This helps to reduce data shuffling across the network during join and reduceByKey operations.
*   **Caching:** While not explicitly used in the final version of the code, caching intermediate RDDs that are used multiple times can significantly improve performance. This was experimented with during development.
*   **Efficient Aggregations:** `reduceByKey` and `aggregateByKey` are used to perform aggregations. These are more efficient than `groupByKey` followed by a map operation because they perform a map-side combine before shuffling the data.
*   **Early Filtering:** In all tasks, the data is filtered as early as possible to reduce the amount of data that needs to be processed in subsequent stages.
*   **Broadcast Variables:** For the `nameBasics` data in tasks 3 and 4, using a broadcast variable for the mapping of nconst to primary name could be a further optimization if the dataset is small enough to fit in memory on each executor. This would avoid a shuffle in the final join.

## How to Run

To run the project, you will need to have sbt installed. You can then run the project using the following command:

```bash
sbt run
```