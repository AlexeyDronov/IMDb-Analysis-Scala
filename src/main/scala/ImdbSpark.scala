package imdb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

object ImdbSpark {
  val conf: SparkConf = new SparkConf()
    .setAppName("ImdbAnalysis")
    .setMaster("local[*]")
    .set("spark.ui.enabled", "false")

  val sc: SparkContext = new SparkContext(conf)

  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath)
    .map(line => ImdbData.parseTitleBasics(line))

  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath)
    .map(line => ImdbData.parseTitleRatings(line))

  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath)
    .map(line => ImdbData.parseTitleCrew(line))

  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath)
    .map(line => ImdbData.parseNameBasics(line))

  def main(args: Array[String]) {
    // Set log level
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    println(durations)

    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    println(titles)

    val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD, titleCrewRDD).collect().toList)
    println(topRated)

    val crews = timed("Task 4", task4(titleBasicsRDD, nameBasicsRDD, titleCrewRDD).collect().toList)
    println(crews)

    sc.stop()
  }

  /**
   * Task 1: Genre Growth Analysis
   * Find the top 5 genres that have experienced the most growth in popularity
   * between 1990-2000 and 2010-2020 periods.
   * Only consider movies and TV shows.
   *
   * @return RDD[(String, Int)] where:
   *         - String is the genre name
   *         - Int is the growth in number of titles
   */
  def task1(rdd: RDD[TitleBasics]): RDD[(String, Int)] = {
    // TODO: Implement this task
    // Expected output example:
    // List((Drama,3504), (Documentary,2883), (Comedy,2439), (Horror,909), (Thriller,854))
    val genreDecadeCounts = titleBasicsRDD
    .filter { title => // filter movies and tvseries, correct year ranges and existing genres
      (title.titleType.getOrElse("NA") == "movie" || title.titleType.getOrElse("NA") == "tvSeries") &&
      title.genres.getOrElse(List("NA")) != List("NA") &&
      (
        (title.startYear.getOrElse(-1) >= 1990 && title.startYear.getOrElse(-1) <= 2000) ||
        (title.startYear.getOrElse(-1) >= 2010 && title.startYear.getOrElse(-1) <= 2020)
      )
    }
    .flatMap { title => // map List(List(genres)) -> List ((genre, decade), 1)
      val decade = if (title.startYear.get >= 1990 && title.startYear.get <= 2000) "9" else "1"
      title.genres.get.distinct.map(genre => ((genre, decade), 1))
    }
    .reduceByKey(_ + _)  // Sum counts for each (genre, decade)

    val top5genres = genreDecadeCounts
    .map{ // transform List((genre, decade), count) -> (genre, (decade, count))
      case ((genre, decade), count) => (genre, (decade, count))
    }
    .groupByKey() 
    .mapValues{ // turn (decade, count) into Map(decade -> count)
      counts => 
      val countMap = counts.toMap
      val countDecade9 = countMap.getOrElse("9", 0)
      val countDecade1 = countMap.getOrElse("1", 0)
      countDecade1 - countDecade9 // calculate the increase 
    }
    .sortBy(-_._2) // sort descending
    .take(5)

    return sc.parallelize(top5genres)
  }

  /**
   * Task 2: Genre Ratings by Decade
   * Identify top 3 genres for each decade based on weighted average ratings.
   * Exclude genres with fewer than 5 entries in each decade.
   *
   * @return RDD[(Int, List[(String, Float)])] where:
   *         - Int is the decade (e.g., 1990)
   *         - List contains tuples of (genre_name, weighted_average_rating)
   */
  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, List[(String, Float)])] = {
    // TODO: Implement this task
    // Expected output format example for one decade:
    // (1990, List((History,8.594199), (Biography,8.496143), (Drama,8.360406)))
    val ratingMap = titleRatingsRDD
    .map{ 
      case TitleRatings(titleId, avgRating, numVotes) => (titleId -> (numVotes, numVotes * avgRating))
    } // get titleId -> (Rating, weightedRating)
    .partitionBy(new HashPartitioner(100))
    .persist()

    val genreRatingsByDecade = titleBasicsRDD
    .filter{ // filter correct titles
      title => (title.startYear.getOrElse(-1) <= 2029 && title.startYear.getOrElse(-1) >= 1900) &&
      title.genres.getOrElse(List("NA")) != List("NA")
    }
    .flatMap{
      title => 
      val roundedDecade = title.startYear.get - (title.startYear.get % 10) // get the decade
      title.genres.get.distinct.map{
        // genre => (roundedDecade,(genre,title.tconst, 0)) // transform to (titleId, (genre, decade))
        genre => (title.tconst, (roundedDecade, genre))
      }
    }
    .join(ratingMap) // 5 sec
    .map{case (titleId, ((decade, genre), ratingInfo)) => ((decade, genre), (ratingInfo))} 
    .groupByKey() // ((decade, genre), (numVotes, weightedRating))
    .filter{case (_, ratingInfo) => (ratingInfo.size >= 5)}
    .aggregateByKey((0, 0.0f))(
      (acc, value) => value.foldLeft(acc) { case ((sumVotes, weightedRatings), (numVotes, weightedRating)) =>
        (sumVotes + numVotes, weightedRatings + weightedRating)
      },
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    .map{
      case ((year, genre), (sumVotes, weightedSum)) => (year, List[(String, Float)]((genre, weightedSum/sumVotes)))
    }
    .reduceByKey{(list1, list2)=>
      (list1 ++ list2).sortBy(-_._2).take(3)
    }
    .sortByKey()
    return genreRatingsByDecade
  }

  /**
   * Task 3: Top Directors
   * Find directors with highest average ratings for their films.
   * Consider only directors with at least 3 films rated above 8.5 with 10,000+ votes.
   *
   * @return RDD[(String, Float)] where:
   *         - String is the director's name
   *         - Float is their average rating
   */
  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings], l3: RDD[TitleCrew]): RDD[(String, Float)] = {
    // TODO: Implement this task
    // Expected output example:
    // List((Tetsurô Araki,9.2), (Matt Shakman,9.066667))
    return sc.parallelize(List(("a", 1)))
  }

  /**
   * Task 4: Prolific Crew Members
   * Identify top 10 most prolific crew members across specific time periods.
   * Consider contributions in 1995-2000, 2005-2010, and 2015-2020.
   *
   * @return RDD[(String, Int)] where:
   *         - String is the crew member's name
   *         - Int is their total number of unique contributions
   */
  def task4(l1: RDD[TitleBasics], l2: RDD[NameBasics], l3: RDD[TitleCrew]): RDD[(String, Int)] = {
    // TODO: Implement this task
    // Expected output example:
    // List((Marco Romano,12), (Jordan Hill,12), ..., (Tony Newton,8))
    return sc.parallelize(List(("a", 1)))
  }

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    println(s"Processing $label took ${stop - start} ms.")
    result
  }
}