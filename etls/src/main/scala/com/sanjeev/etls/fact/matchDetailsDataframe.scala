package com.sanjeev.etls.fact

import com.sanjeev.utilities.Utilities

object matchDetailsDataframe extends Utilities {

  val spark = createSparkSession("cricStats", false)

  val matchData  = spark.read.option("multiline", "true").json("/Users/sreesanjeev/Projects/cricstats/etls/src/main/resources/6401.json")

  matchData.printSchema()

}
