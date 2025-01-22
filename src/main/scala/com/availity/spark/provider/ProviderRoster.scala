package com.availity.spark.provider

import org.apache.spark.sql.functions.{coalesce, col, concat, count, date_format, lit, month, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Performs aggregations over provider and visit data
 * @param ss spark session object
 * @param providerPath path to the provider file
 * @param visitPath path to the visits file
 */
class ProviderRoster(ss: SparkSession, providerPath: String, visitPath: String)  {

  /**
   * Reads CSV data into a dataframe suitable for downstream operations
   * @return DataFrame of joined provider and visit data
   */
  def readData(): DataFrame = {

    // read in the providers.csv
    def providers: DataFrame = ss.read.format("csv")
      .options(Map("header" -> "true", "delimiter" -> "|"))
      .load(providerPath)
      .withColumn("provider_id_int", coalesce(col("provider_id").cast("integer"), lit(0)))
      .drop("provider_id")
      .withColumnRenamed("provider_id_int", "provider_id")


    // read in the visits.csv
    def visits: DataFrame = ss.read.format("csv")
      .options(Map("header" -> "false"))
      .load(visitPath)
      .withColumn("visit_id", coalesce(col("_c0").cast("integer"), lit(0)))
      .withColumn("provider_id", coalesce(col("_c1").cast("integer"), lit(0)))
      .withColumn("date_of_service", date_format(col("_c2"), "yyyy-MM-dd"))
      .drop("_c0", "_c1", "_c2")

    // join providers with their visits
    providers.join(visits, Seq("provider_id"), "left_outer")
  }

  /**
   * Given the two data datasets, calculate the total number of visits per provider.
   * The resulting set should contain the provider's ID, name, specialty, along with
   * the number of visits.
   *
   * @param df Input dataframe
   * @return Aggregated data
   */
  def getProviderVisitCount(df: DataFrame): DataFrame = {
    df.withColumn("provider_name", concat(col("last_name"), lit(", "), col("first_name"), lit(" " ), col("middle_name")))
      .groupBy("provider_id", "provider_name", "provider_specialty")
      .agg(count(col("visit_id")).alias("number_of_visits"))
      .orderBy("provider_specialty", "provider_id", "provider_name")
  }

  /**
   * Given the two datasets, calculate the total number of visits per provider per month.
   * The resulting set should contain the provider's ID, the month, and total number of visits.
   *
   * @param df Input dataframe
   * @return Aggregated data
   */
  def getProviderVisitCountPerMonth(df: DataFrame): DataFrame = {
    df
      .withColumn("month_of_service", date_format(col("date_of_service"), "MMM"))
      .filter(col("visit_id").isNotNull)
      .groupBy("provider_id", "month_of_service")
      .agg(count(lit(1)).alias("number_of_visits"))
      .orderBy("provider_id", "month_of_service")
  }
}

/**
 * Application entrypoint
 */
object Main extends App {

  private val dataPath = "C:\\Users\\cashcraft\\IdeaProjects\\provider-roster\\data\\"
  private val providerPath = dataPath + "providers.csv"
  private val visitPath = dataPath + "visits.csv"

  private val ss = new SparkSession.Builder().config("spark.master", "local").appName("my_app").getOrCreate()

  // spark session and file paths are parameterizable to simplify unit testing
  private val rObj = new ProviderRoster(ss, providerPath, visitPath)

  // we'll reuse this dataframe for both operations to reduce IO and joins
  private val providerVisits = rObj.readData()

  // perform the aggregations
  private val visitsPerProvider = rObj.getProviderVisitCount(providerVisits)
  private val visitsPerProviderPerMonth = rObj.getProviderVisitCountPerMonth(providerVisits)

  // output the visits per provider in json, partitioned by the provider's specialty.
  visitsPerProvider
    .repartition(1)  // we only want a single file per partition
    .write
    .format("json")
    .mode("overwrite")
    .partitionBy("provider_specialty")
    .save("output/visitsPerProvider")

  // output the visits per provider per month in json
  visitsPerProviderPerMonth
    .repartition(1) // we only want a single file output
    .write
    .format("json")
    .mode("overwrite")
    .save("output/visitsPerProviderPerMonth")

  ss.stop()
}