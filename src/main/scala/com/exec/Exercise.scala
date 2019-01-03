package com.exec

import org.apache.spark.sql._

object Exercise {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Exercise")
      .config("spark.master", "local[*]")
      .getOrCreate()
    import spark.sqlContext.implicits._

    // Exercise 1
    spark.read.option("header", "true")
      .csv("./src/main/resources/exercise1/Sacramentorealestatetransactions.csv")
      .select("street")
      .write.mode(SaveMode.Overwrite).parquet("./parquet/houses")
    spark.read.option("header", "true")
      .csv("./src/main/resources/exercise1/SacramentocrimeJanuary2006.csv")
      .select("cdatetime", "address", "crimedescr")
      .write.mode(SaveMode.Overwrite).parquet("./parquet/crimes")

    val houses = spark.read.parquet("./parquet/houses")
    println(s"\n${houses.count} houses were sold.\n")

    val crimes = spark.read.parquet("./parquet/crimes")
    println(s"\n${crimes.count} crimes occurred.\n")

    houses.createOrReplaceTempView("houses")
    crimes.createOrReplaceTempView("crimes")
    val crimesInHouses = spark.sql(
      """
        |SELECT cdatetime, address, crimedescr FROM crimes, houses
        |WHERE crimes.address = houses.street
      """.stripMargin)

    println(s"\nThere were ${crimesInHouses.count} crimes occurred in the houses sold and they were:")
    crimesInHouses.show()

    // Exercise 2
    spark.read.option("header", "true")
      .csv("./src/main/resources/exercise2/*.csv")
      .select("ReportDate", "OccurenceZipCode", "PrimaryViolation")
      .write.mode(SaveMode.Overwrite).parquet("./parquet/crimes_year")
    val crimesYear = spark.read.parquet("./parquet/crimes_year")
    crimesYear.createOrReplaceTempView("crimes_year")

    println("\nZip codes with the most crime")
    spark.sql(
      """
        |SELECT OccurenceZipCode, count(*) AS count
        |FROM crimes_year
        |GROUP BY OccurenceZipCode
        |ORDER BY count DESC
      """.stripMargin).show(5)


    println("\nMost committed crimes in zip codes")
    spark.sql(
      """
        |SELECT OccurenceZipCode, PrimaryViolation, count(PrimaryViolation) AS count
        |FROM crimes_year
        |GROUP BY OccurenceZipCode, PrimaryViolation
        |ORDER BY count DESC
      """.stripMargin).show(5)

    println("\nCrimes happened during the year")
    spark.sql(
      """
        |SELECT PrimaryViolation, count(PrimaryViolation) AS count
        |FROM crimes_year
        |GROUP BY PrimaryViolation
        |ORDER BY count DESC
      """.stripMargin).show(5)

    println("\nCrimes ordered by month")
    spark.sql("SELECT ReportDate FROM crimes_year")
      .map(r => r.getString(0).split("/")(0))
      .groupBy('value).count
      .toDF("month", "count")
      .orderBy('count.desc).show(5)
  }
}
