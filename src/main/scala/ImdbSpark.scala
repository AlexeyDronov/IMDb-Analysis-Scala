package imdb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

object ImdbSpark {
  val conf: SparkConf = new SparkConf()
    .setAppName("ImdbAnalysis")
    .setMaster("local[*]")
    .set("spark.driver.memory", "12g")
    .set("spark.default.parallelism", "20")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.ui.enabled", "false")

  val sc: SparkContext = new SparkContext(conf)

  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath)
    .filter(line => !line.startsWith("tconst"))
    .map(line => ImdbData.parseTitleBasics(line))
    .cache()

  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath)
    .filter(line => !line.startsWith("tconst"))
    .map(line => ImdbData.parseTitleRatings(line))
    .cache()

  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath)
    .filter(line => !line.startsWith("tconst"))
    .map(line => ImdbData.parseTitleCrew(line))
    .cache()

  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath)
    .filter(line => !line.startsWith("nconst"))
    .map(line => ImdbData.parseNameBasics(line))
    .cache()

  // --- Statistics helpers --------------------------------------
  def mean(xs: Seq[Double]): Double = xs.sum / xs.length

  def median(xs: Seq[Double]): Double = {
    val sorted = xs.sorted
    if (xs.length % 2 == 1) sorted(xs.length / 2)
    else (sorted(xs.length / 2 - 1) + sorted(xs.length / 2)) / 2.0
  }

  def stdev(xs: Seq[Double]): Double = {
    val m = mean(xs)
    math.sqrt(xs.map(x => math.pow(x - m, 2)).sum / xs.length)
  }

  // --- Timing helper ------------------------------------------
  def timedTask[T](fn: => T): (T, Double) = {
    val start = System.nanoTime()
    val result = fn
    val elapsedMs = (System.nanoTime() - start) / 1e6
    (result, elapsedMs)
  }

  def main(args: Array[String]): Unit = {
    // Set log level
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    // Parse CLI arguments for trials (defaults to 1)
    val trials = if (args.length > 0) args(0).toInt else 1
    val verbose = args.contains("-v") || args.contains("--verbose")

    println("Materializing RDD caches...")
    // Force cache materialization so disk I/O isn't counted in the first benchmark trial
    titleBasicsRDD.count()
    titleRatingsRDD.count()
    titleCrewRDD.count()
    nameBasicsRDD.count()

    // Define tasks as zero-argument functions to pass around easily.
    // Notice we call .collect() here so the action is included in the timed block!
    val tasks = Seq(
      ("Task 1", () => task1(titleBasicsRDD).collect()),
      ("Task 2", () => task2(titleBasicsRDD, titleRatingsRDD).collect()),
      ("Task 3", () => task3(nameBasicsRDD, titleRatingsRDD, titleCrewRDD).collect()),
      ("Task 4", () => task4(titleBasicsRDD, nameBasicsRDD, titleCrewRDD).collect())
    )

    if (trials == 1) {
      // Run Once Mode
      tasks.foreach { case (name, fn) =>
        val (result, duration) = timedTask(fn())
        println(f"$name: $duration%.2f ms")
        if (verbose) {
          result.asInstanceOf[Array[_]].foreach(r => println(s"    $r"))
          println("-" * 40)
        }
      }
    } else {
      // Benchmark Mode
      println("Warming up...")
      tasks.foreach { case (_, fn) => fn() }

      val times = scala.collection.mutable.Map[String, List[Double]]().withDefaultValue(Nil)

      for (i <- 1 to trials) {
        println(s"Running trial $i...")
        tasks.foreach { case (name, fn) =>
          val (_, duration) = timedTask(fn())
          times(name) = times(name) :+ duration
        }
      }

      println("\nBenchmark results:")
      tasks.map(_._1).foreach { name =>
        val values = times(name)
        println(f"$name: mean=${mean(values)}%.2f ms, median=${median(values)}%.2f ms, std=${stdev(values)}%.2f ms.")
      }
    }
    // scala.io.StdIn.readLine("Press Enter to exit and close the Spark UI...")
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
    val top5genres = rdd
    .filter { title => // filter movies and tvseries and correct year ranges
      (title.titleType.exists(title => title == "movie" || title == "tvSeries")) &&
      (title.genres.isDefined) &&
      (title.startYear.exists(year => (year >= 1990 && year <= 2000) || (year >= 2010 && year <= 2020)))
    }
    .flatMap { title => // flatten to get count of (genre, decade)
      val decade = if (title.startYear.get >= 1990 && title.startYear.get <= 2000) "9" else "1"
      title.genres.get.distinct.map(genre => ((genre, decade), 1))
    }
    .partitionBy(new HashPartitioner(100))
    .reduceByKey(_ + _)  // Sum counts for each (genre, decade)
    .map{ // transform ((genre, decade), count) -> (genre, (decade, count))
      case ((genre, decade), count) => (genre, (decade, count))
    }
    .groupByKey() // group by genre
    .mapValues{ 
      counts => 
      val countMap = counts.toMap // turn (decade, count) into Map(decade -> count)
      val countDecade9 = countMap.getOrElse("9", 0)
      val countDecade1 = countMap.getOrElse("1", 0)
      countDecade1 - countDecade9 // calculate the increase 
    }
    .takeOrdered(5)(Ordering[Int].reverse.on(_._2)) // take top 5 in descending order
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
    val ratingMap = l2
    .map{ 
      case TitleRatings(titleId, avgRating, numVotes) => (titleId -> (numVotes, numVotes * avgRating))
    } // get titleId -> (Rating, weightedRating)
    .partitionBy(new HashPartitioner(100))


    val genreRatingsByDecade = l1
    .filter{ // filter correct titles
      title => title.startYear.exists(year => year >= 1900 && year <= 2029) &&
                title.genres.isDefined
    }
    .flatMap{
      title => 
      val roundedDecade = title.startYear.get - (title.startYear.get % 10) // get the decade
      title.genres.get.distinct.map{
        genre => (title.tconst, (roundedDecade, genre)) // transform to (titleId, (genre, decade))
      }
    }
    .partitionBy(new HashPartitioner(100))
    .join(ratingMap) // join
    // (decade, genre), (numVotes, weightedAvgRating, 1)
    .map{case (titleId, ((decade, genre), (numVotes, weightedAvgRating))) => ((decade, genre), (numVotes, weightedAvgRating, 1))} 
    .reduceByKey{ // sum the numVotes, weithedRating and counts for each pair of (decade, genre)
      case ((numVotes1, weightedAvg1, count1), (numVotes2, weightedAvg2, count2)) => 
      (numVotes1 + numVotes2, weightedAvg1 + weightedAvg2, count1 + count2)
    }
    .filter{ // exclude genres with <= 5 entries
      case ((decade, genre), (numVotes, weightedAvgRating, count)) => count >= 5
    }
    .mapValues{ // calculate the weighted rating
      case (numVotes, weightedRating, _) => weightedRating / numVotes
    }
    .map{
      case ((decade, genre), rating) => (decade, (genre, rating))
    }
    .groupByKey() // group by decade
    .mapValues(genreRating => genreRating.toList.sortBy(-_._2).take(3)) // take top 3 genres descending
    .sortByKey() // sort by decade ascending 

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
  def task3(l1: RDD[NameBasics], l2: RDD[TitleRatings], l3: RDD[TitleCrew]): RDD[(String, Float)] = {
    val filtered_directors = l1
    .filter(crew => (crew.primaryName.isDefined))
    .map{ // obtain (crewId, primaryName)
      crew => (crew.nconst, crew.primaryName.get)
    }


    val qualifying_titles = l2
    .filter(title => (title.numVotes >= 10000 && title.averageRating > 8.5))
    .map{
      title => (title.tconst, title.averageRating) // obtain (titleId, avgRating)
    }
    .partitionBy(new HashPartitioner(100))


    val topDirectors = l3
    .filter(crew => crew.directors.isDefined)
    .flatMap{
      crew => crew.directors.get.map{
        director => (crew.tconst, director) // obtain unpacked (titleId, directorId)
      }
    }
    .partitionBy(new HashPartitioner(100))
    .join(qualifying_titles) // join on title Id to get directorId and avgRating
    .map{ // map to reduce by key and keep counts for averaging
      case (titleId, (directorId, movieRating)) => (directorId, (movieRating,1)) 
    }
    .reduceByKey{ // sum the movie ratings and movie counts for each director
      case ((rating1, count1), (rating2, count2)) => (rating1+rating2, count1+count2)
    }
    .filter{case (_, (_, count)) => count >= 3}  // has to have at least 3 titles
    .map{case (directorId, (rating, count)) => (directorId, rating/count)} // calculate average rating
    .join(filtered_directors) // join to get the directors' names
    .map{case (directorId, (rating, name)) => (name, rating)}
    .takeOrdered(10)(Ordering[(Double, String)].on(x => (-x._2, x._1))) // take top 10 with a secondary sort on name

    return sc.parallelize(topDirectors)
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
    val titleBasicsFiltered = l1
        .filter{
          title => title.startYear.exists(year => (year >= 1995 && year <= 2000) || (year >= 2005 && year <= 2010) || 
          (year >= 2015 && year <= 2020)) 
        }
        .map(title => (title.tconst, 1)) // get (titleId, releaseYear)
        .partitionBy(new HashPartitioner(100))

    val nameBasicsFiltered = l2
        .filter(crew => crew.primaryName.isDefined)
        .map(crew => (crew.nconst, crew.primaryName.get)) // get (crewId, name)
        .partitionBy(new HashPartitioner(100))

    val titleCrewFiltered = l3
        .filter(crew => crew.directors.isDefined || crew.writers.isDefined)
        .map{ // concatenate directors and writers and select distinct
          titleCrew => (titleCrew.tconst, (titleCrew.directors.getOrElse(List.empty[String]) ++ titleCrew.writers.getOrElse(List.empty[String])).toSet)
        } 
        .flatMap{ // get (titleId, crewId)
          case (tconst, crewId) => crewId.map(crew => (tconst, crew))
        }
        .partitionBy(new HashPartitioner(100))
        .join(titleBasicsFiltered) // join on tconst
        .partitionBy(new HashPartitioner(100))
        .map{
          case (_, (crewId, _)) => (crewId, 1)
        }
        .reduceByKey(_ + _) // get counts for each crew member
        .join(nameBasicsFiltered) // join on nconst to get crew member names
        .map{ 
          case (_, (count, name)) => (name, count)
        }
        .sortBy(s => (-s._2, s._1)) // sort first by rating, second by name alphabetically
        .take(10)
    return sc.parallelize(titleCrewFiltered)
  }

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    println(s"Processing $label took ${stop - start} ms.")
    result
  }
}