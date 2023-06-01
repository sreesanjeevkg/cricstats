package com.sanjeev.utilities

import org.apache.spark.sql._

/**
 * @author ${user.name}
 */
class Utilities {
  def createSparkSession(appName: String, verboseLogging: Boolean, logWarnings: Boolean = false): SparkSession = {
    val builder = SparkSession.builder()
    val session = builder
      .appName(appName)
      .config("spark.master", "local")
      .getOrCreate()

    val loggingLevel = if (logWarnings) {
      "WARN"
    }
    else if (verboseLogging) {
      "DEBUG"
    }
    else {
      "ERROR"
    }
    session.sparkContext.setLogLevel(loggingLevel)
    session
  }
  def readData(pathName: String, spark: SparkSession): DataFrame = {
    spark.read.options(Map("header" -> "true", "delimiter" -> ",")).csv(pathName)
  }


}
