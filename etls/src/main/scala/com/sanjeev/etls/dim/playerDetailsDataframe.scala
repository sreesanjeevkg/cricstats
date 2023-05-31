package com.sanjeev.etls.dim

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import com.sanjeev.utilities.Utilities

/**
 * @author ${user.name}
 */
object playerDetailsDataframe extends App {

  val spark  = Utilities.createSparkSession("cricStats", false)

  import spark.implicits._

  spark.sql("set spark.sql.caseSensitive=true")

  val cricData = spark.read.options(Map("header" -> "true", "delimiter" -> ","))
    .csv("/Users/sreesanjeev/Projects/cricstats/etls/src/main/resources/Players.csv")

//  cricData.as("c")
//    .select($"FULL NAME", $"PLAYING ROLE", $"FIELDING POSITION", $"BATTING STYLE", $"BOWLING STYLE", $"player_identifier")
//    .where($"player_identifier" === "ba607b88")
//    .show(false)

  cricData.select($"Full Name", $"FULL NAME").where($"Full Name" isNotNull).show(false)

}
