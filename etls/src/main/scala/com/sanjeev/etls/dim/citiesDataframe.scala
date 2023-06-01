package com.sanjeev.etls.dim

import com.sanjeev.utilities.Utilities
import com.sanjeev.model.stadiumInfo
import org.apache.spark.sql.functions.monotonically_increasing_id

object citiesDataframe extends Utilities {

  val spark = createSparkSession("cricStats", false)

  import spark.implicits._

  spark.sql("set spark.sql.caseSensitive=true")

  val matchData = spark.read.option("multiline", "true").json("/Users/sreesanjeev/Projects/cricstats/etls/src/main/resources/all_json/*.json")

  matchData.select($"info.city")
    .distinct()
    .withColumn("cityID", monotonically_increasing_id().cast("Int"))
    .as[stadiumInfo]
    .show(false)

}
