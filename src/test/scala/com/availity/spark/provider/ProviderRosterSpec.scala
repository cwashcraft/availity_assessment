package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

import java.nio.file.Paths
import java.util.Objects

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  @transient var ss: SparkSession = null

  // read test data from resource location
  val providersPath: String = Paths.get(Objects.requireNonNull(this.getClass.getClassLoader.getResource("test-providers.csv")).toURI).toAbsolutePath.toString
  val visitsPath: String = Paths.get(Objects.requireNonNull(this.getClass.getClassLoader.getResource("test-visits.csv")).toURI).toAbsolutePath.toString

  override def beforeEach(): Unit = {
    ss = new SparkSession.Builder()
      .config("spark.master", "local")
      .appName("test_app")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    ss.stop()
  }

  describe("process") {

    it ("visitCountsPerProvider") {

      // define our expected output
      val testData = Seq(
        Row(2, "Block, Candice C", "Cardiology", 9L),
        Row(7, "Barrows, Napoleon A", "Cardiology", 4L),
        Row(8, "Reichel, Roy C", "Chiropractic", 1L),
        Row(9, "Stokes, Lavon C", "Dermatology", 10L),
        Row(4, "Brown, Lenny B", "Gastroenterology", 6L),
        Row(10, "Leannon, Tiara A", "General Practice", 6L),
        Row(3, "Bartell, Margaret B", "Psychiatry", 3L),
        Row(5, "Daugherty, Julien B", "Rheumatology", 0L),
        Row(6, "Mertz, Rowena A", "Sports Medicine", 4L),
        Row(1, "Kuphal, Bessie B", "Urology", 12L),
      )

      val testSchema = List(
        StructField("provider_id", IntegerType, nullable = false),
        StructField("provider_name", StringType, nullable = true),
        StructField("provider_specialty", StringType, nullable = true),
        StructField("number_of_visits", LongType, nullable = false)
      )

      val expectedDF = ss.createDataFrame(ss.sparkContext.parallelize(testData), StructType(testSchema))

      val rObj = new ProviderRoster(ss, providersPath, visitsPath)
      val providerVisits = rObj.readData()
      val actualDF = rObj.getProviderVisitCount(providerVisits)

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it ("visitCountsPerProviderPerMonth") {

      // define our expected output
      val testData = Seq(
        Row(1, "Apr", 1L),
        Row(1, "Aug", 1L),
        Row(1, "Dec", 2L),
        Row(1, "Feb", 1L),
        Row(1, "Jul", 3L),
        Row(1, "Jun", 1L),
        Row(1, "Mar", 1L),
        Row(1, "Oct", 2L),
        Row(2, "Apr", 1L),
        Row(2, "Dec", 1L),
        Row(2, "Jan", 1L),
        Row(2, "Jun", 1L),
        Row(2, "May", 1L),
        Row(2, "Nov", 2L),
        Row(2, "Oct", 1L),
        Row(2, "Sep", 1L),
        Row(3, "Apr", 1L),
        Row(3, "Jul", 1L),
        Row(3, "Nov", 1L),
        Row(4, "Jul", 3L),
        Row(4, "Nov", 1L),
        Row(4, "Oct", 2L),
        Row(6, "Jan", 1L),
        Row(6, "Jul", 1L),
        Row(6, "Oct", 1L),
        Row(6, "Sep", 1L),
        Row(7, "Aug", 1L),
        Row(7, "Jul", 1L),
        Row(7, "May", 1L),
        Row(7, "Oct", 1L),
        Row(8, "Jul", 1L),
        Row(9, "Apr", 1L),
        Row(9, "Aug", 1L),
        Row(9, "Jan", 2L),
        Row(9, "Jul", 2L),
        Row(9, "Jun", 2L),
        Row(9, "Mar", 1L),
        Row(9, "Oct", 1L),
        Row(10, "Dec", 4L),
        Row(10, "Jul", 1L),
        Row(10, "Oct", 1L)
      )

      val testSchema = List(
        StructField("provider_id", IntegerType, nullable = false),
        StructField("month_of_service", StringType, nullable = true),
        StructField("number_of_visits", LongType, nullable = false)
      )

      val expectedDF = ss.createDataFrame(ss.sparkContext.parallelize(testData), StructType(testSchema))

      val rObj = new ProviderRoster(ss, providersPath, visitsPath)
      val providerVisits = rObj.readData()
      val actualDF = rObj.getProviderVisitCountPerMonth(providerVisits)

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }
  }
}
