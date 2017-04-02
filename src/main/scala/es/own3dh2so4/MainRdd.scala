package es.own3dh2so4

import org.apache.spark.sql.SparkSession

/**
  * Created by david on 1/04/17.
  */
object MainRdd extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("")

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  //Working with rdd
  import spark.implicits._
  val movies = spark.read.option("header","true").csv(inputFiles).as[Movie]

  println("Number of movies: " + movies.count())

  val moviesCountries = movies.filter(_.country != null).map( m => (m.country,1) ).rdd.
    reduceByKey(_+_).sortBy(_._2,ascending = false).take(5)

  println("Top 5 countries:")
  println(moviesCountries.mkString("\n"))

  val bestActor = movies.map(m => (m.actor_1_name,Option(m.actor_1_facebook_likes).getOrElse("0").toDouble)).
    union(movies.map(m => (m.actor_2_name,Option(m.actor_2_facebook_likes).getOrElse("0").toDouble))).
    union(movies.map(m => (m.actor_3_name,Option(m.actor_3_facebook_likes).getOrElse("0").toDouble))).rdd.
    reduceByKey(_+_).sortBy(_._2,ascending = false).take(5)

  println("Top 5 actors:")
  println(bestActor.mkString("\n"))

  val bestFilmCountry = movies.filter(_.country != null).map( m => (m.country,(m.movie_facebook_likes,m.movie_title))).rdd.
    reduceByKey((x,y) => if (x._1 > y._1 ) x else y).collect()

  println("Best films countries")
  println(bestFilmCountry.mkString("\n"))

  val bestFilmGenres = movies.filter(_.genres != null).
    flatMap( m => for (g <- m.genres.split('|')) yield (g,(m.movie_facebook_likes,m.movie_title))).rdd.
    reduceByKey((x,y) => if (x._1 > y._1 ) x else y).collect()

  println("Best films genres")
  println(bestFilmGenres.mkString("\n"))


}
