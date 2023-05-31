package com.sanjeev.etls.fact

import com.sanjeev.utilities.Utilities

object matchDetailsDataframe extends App {

  val spark = Utilities.createSparkSession("cricStats", false)

  import spark.implicits._

  spark.sql("set spark.sql.caseSensitive=true")

  val matchData  = spark.read.option("multiline", "true").json("/Users/sreesanjeev/Projects/cricstats/etls/src/main/resources/6401.json")

  matchData.printSchema()



}
