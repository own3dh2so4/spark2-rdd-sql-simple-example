package es.own3dh2so4

import org.apache.spark.sql.SparkSession

/**
  * Created by david on 2/04/17.
  */
object MainSql extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("")

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()

  //Working with rdd
  val movies = spark.read.option("header","true").option("inferSchema","true").csv(inputFiles)
  movies.printSchema()
  movies.explain()

  movies.createOrReplaceTempView("MoviesTable")

  val result = spark.sql("SELECT movie_title, country FROM MoviesTable")
  result.show(5)
  result.head(3)


  val result2 = spark.sql("SELECT count(*) as num_movies, country FROM MoviesTable GROUP BY country ORDER BY count(*) DESC")
  result2.show(5)
  result2.head(3)

  val result3 = spark.sql("SELECT movie_title FROM MoviesTable WHERE country = 'Spain'")
  result3.show(5)
  result3.head(3)
}
