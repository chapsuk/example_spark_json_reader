package com.example

import org.apache.spark.sql.SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object CrimeStrict {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      throw new IllegalArgumentException("Expected 3 args, found: "+ args.length +
        "Usage: <jar> <crime.csv> <offense_codes.csv> <output_folder>")
    }

    val crimesFile = args(0)
    val offenseFile = args(1)
    val outputDir = args(2)

    val spark = SparkSession
      .builder()
      .appName("CrimeStrict")
      .getOrCreate()

    import spark.implicits._

    val crimesDF = spark.read.option("header", "true").csv(crimesFile).na.fill("__NOT_SET", Seq("DISTRICT"))
    val offenseDF = spark.read.option("header", "true").csv(offenseFile)

    crimesDF.createOrReplaceTempView("crimes")
    offenseDF.createOrReplaceTempView("offense")

    // crimes_monthly
    val monthly = crimesDF
      .sqlContext
      .sql("SELECT coalesce(t.DISTRICT, '__NOT_SET') as district, " +
        "percentile_approx(t.cnt, 0.5, 100) as crimes_monthly FROM " +
        "(SELECT DISTRICT, count(*) as cnt from crimes GROUP BY DISTRICT, YEAR, MONTH) as t GROUP BY t.DISTRICT")

    val freqTop = crimesDF
      .sqlContext
      .sql("""
          |  SELECT
          |     t.district,
          |     t.offense_code,
          |     t.rank
          |  FROM(
          |     SELECT
          |       coalesce(g.DISTRICT, '__NOT_SET') as district,
          |       cast(g.OFFENSE_CODE as int) as offense_code,
          |       rank() OVER (
          |         PARTITION BY g.DISTRICT ORDER BY g.cnt DESC
          |       ) as rank
          |     FROM (
          |       SELECT
          |         DISTRICT,
          |         OFFENSE_CODE,
          |         count(*) as cnt
          |       FROM crimes
          |       GROUP BY DISTRICT, OFFENSE_CODE
          |      ) AS g
          |  ) AS t
          |  WHERE t.rank <= 3
          |""".stripMargin)

    val normalizedOffenseDF = offenseDF.sqlContext.sql("""
       |SELECT t.code, trim(split(t.name, '-')[0]) as name
       |FROM (SELECT cast(CODE as int) as code, last(NAME) as name FROM offense GROUP BY CODE) as t
       |""".stripMargin)

    val freq = freqTop
      .join(
        broadcast(normalizedOffenseDF),
        freqTop("offense_code") <=> normalizedOffenseDF("code")
      )
      .orderBy("district", "rank")
      .groupBy("district")
      .agg(collect_list("name") as "frequent_crime_types")

    val groupByDistrict = crimesDF
      .withColumn("Lat", $"Lat".cast(DoubleType))
      .withColumn("Long", $"Long".cast(DoubleType))
      .groupBy($"DISTRICT" as "district")
      .agg(
        count($"INCIDENT_NUMBER") as "crimes_total",
        avg($"Lat") as "lat",
        avg($"Long") as "lng")

    groupByDistrict
      .join(monthly, "district")
      .join(freq, "district")
      .orderBy("district")
      .repartition(1)
      .write
      .mode("append")
      .option("compression", "none")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", false)
      .parquet(outputDir)
  }
}

