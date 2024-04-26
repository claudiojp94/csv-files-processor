package pt.claudiopereira.csvfilesprocessor

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object Main {
  def main(args: Array[String]): Unit = {
    // Property setting because of a compilation error, not sure if only occurs on Windows or other OS too
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("csv-files-processor")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // Reading of CSV files into DataFrame
    val df_user_reviews = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .csv("data/googleplaystore_user_reviews.csv")

    val df_apps = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .csv("data/googleplaystore.csv")

    // Part 1
    val df_1 = part1(df_user_reviews)

    // Part 2
    val df_2 = part2(df_apps, 4.0)

    // Part 3
    val df_3 = part3(df_apps)

    // Part 4
    part4(df_3, df_1)

    // Part 5
    val df_4 = part5(df_1, df_3)

    // Opted to show every result, except part4(which is not required to define a value for), even if not asked in the task
    df_1.show()
    df_2.show()
    df_3.show()
    df_4.show()

    spark.stop()
  }

  /*
  Part 1
  Function to calculate average sentiment polarity
   */
  private def part1(df: DataFrame): DataFrame = {
    df.groupBy("App")
      .agg(avg(col("Sentiment_Polarity")).cast(sql.types.DoubleType).alias("Average_Sentiment_Polarity"))
      .na.fill(0, Seq("Average_Sentiment_Polarity"))
  }

  /*
  Part 2
  Function that obtains all Apps with a rating >= ratingThreshold sorted in descending order and save it as a CSV
   */
  private def part2(df: DataFrame, ratingThreshold: Double): DataFrame = {
    // Had to convert the column to Double because of errors in sorting and NaN values not being dropped
    val dfNumeric = df.withColumn("Rating", col("Rating").cast(sql.types.DoubleType))

    // Filtering by the ratingThreshold and eliminating NaN results
    val dfFiltered = dfNumeric.where(col("Rating") >= ratingThreshold).na.drop(Seq("Rating"))

    // Sorting in descending order
    val dfSorted = dfFiltered.orderBy(col("Rating").desc)

    // Writing to a CSV file, using the required delimiter. Turns out this doesn't write into a single file as I was expecting
    dfSorted.write
      .option("delimiter", "§")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("data/best_apps.csv")

    dfSorted
  }

  /*
  Part 3
  Creation of a new Dataframe with the following structure:
    - App is a unique value
    - In case of App duplicates, the column Categories contains an array with all the possible categories
      (without duplicates) for each App
    - In case of App duplicates, for the remaining columns, the values are the ones on the row with the maximum
      number of reviews
    Renamed some columns and changed some Data types as well as some Default Values
    Made the necessary conversions of values
   */
  private def part3(df: DataFrame): DataFrame = {
    // Aggregate categories into a single row and List(without duplicates), while dropping App duplicates
    val aggregatedDf = df.groupBy("App")
      .agg(collect_list("Category").alias("Categories"))
      .withColumn("Categories", array_distinct(col("Categories")))
      .dropDuplicates("App")

    // Getting the highest value from Reviews and putting the default value at 0
    val maxReviewsDf = df.groupBy("App")
      .agg(functions.max("Reviews").alias("Reviews"))
      .na.fill(0, Seq("Reviews"))

    // Joining the original Dataframe with the new ones
    val joinedDf = df.join(maxReviewsDf, Seq("App", "Reviews"), "inner")
      .join(aggregatedDf, Seq("App"), "inner")
      .drop("Category")

    // Adjustments to the other columns
    // Couldn't make all of the default values into null like asked,.na.fill requires a value, opted to put
    // a "NULL" string instead. On Size and Price I injected a None in case of invalid value or unknown format
    // which translates into a null
    val sizeToDoubleUDF = udf(convertSizeToDouble _)
    val priceToDoubleUDF = udf(convertPriceToDoubleAndEur _)
    val resultDf = joinedDf
      .withColumn("Rating", col("Rating").cast(sql.types.DoubleType))
      .withColumn("Size", sizeToDoubleUDF(col("Size")))
      .withColumn("Price",priceToDoubleUDF(col("Price")))
      .withColumn("Genres", functions.split(col("Genres"), ";"))
      .withColumn("Last_Updated", to_date(col("Last Updated"), "MMMM d, yyyy"))
      .withColumn("Reviews", col("Reviews").cast(sql.types.LongType))
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .drop("Last Updated")
      .na.fill("NULL", Seq(
        "Rating", "Installs", "Type", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version"
      ))

    // Reorganize columns in the desired order
    val reorderedDf = resultDf.select(
      col("App"), col("Categories"), col("Rating"), col("Reviews"), col("Size"), col("Installs"),
      col("Type"), col("Price"), col("Content_Rating"), col("Genres"), col("Last_Updated"),
      col("Current_Version"), col("Minimum_Android_Version")
    )

    reorderedDf
  }

  /*
  Part 4
  Same Dataframe as Part 3 with Average_Sentiment_Polarity included
   */
  private def part4(df1: DataFrame, df2: DataFrame): DataFrame = {
    // Join both Dataframes
    val joinedDf = df1.join(df2, Seq("App"), "left")

    // Write to a parquet file with gzip compression
    joinedDf.write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .parquet("data/googleplaystore_cleaned")

    joinedDf
  }

  /*
  Part 5
  Function that calculates number of applications, the average rating and the
  average sentiment polarity by genre, and save it as a parquet file with gzip compression
   */
  private def part5(df_1: DataFrame, df_3: DataFrame): DataFrame = {
    // Splitting the Genres array into single variables, while fetching the other necessary columns
    val explodedDf = df_3.select(explode(col("Genres")).alias("Genres"), col("App"), col("Rating"))

    // Joining with df_1 which has the Average_Sentiment_Polarity
    val joinedDf = explodedDf.join(df_1, Seq("App"), "left")

    // Construction of the final Dataframe that counts the number of Apps, average rating and
    // average sentiment polarity for each genre
    val finalDf = joinedDf.groupBy("Genres")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )
      .na.fill(0, Seq("Average_Sentiment_Polarity"))

    // Write to a parquet file with gzip compression
    finalDf.write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .parquet("data/googleplaystore_metrics")

    finalDf

  }

  /*
  Private function that converts size to double and KB to MB
   */
  private def convertSizeToDouble(size : String): Option[Double] = {
    if (size != null) {
      if (size.endsWith("M")) {
        Some(size.dropRight(1).toDouble)
      } else if (size.endsWith("k")) {
        Some(size.dropRight(1).toDouble / 1024) // Convert KB to MB
      } else {
        None // Default value for invalid or unknown format
      }
    } else {
      None // Default value for invalid or unknown format
    }
  }

  /*
  Private function that converts price to double and USD to EUR
   */
  private def convertPriceToDoubleAndEur(price: String): Option[Double] = {
    if (price != null && price.startsWith("$") && price.drop(1).toDouble > 0) {
      Some(price.drop(1).toDouble * 0.9) // Convert USD to EUR
    } else {
      None // Default value for invalid or unknown format
    }
  }
}