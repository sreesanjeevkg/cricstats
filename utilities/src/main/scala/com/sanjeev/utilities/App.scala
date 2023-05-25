package com.sanjeev.utilities

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  private def createSparkSession(appName: String, verboseLogging: Boolean, logWarnings: Boolean = false): SparkSession = {
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
      "INFO"
    }
    session.sparkContext.setLogLevel(loggingLevel)
    session
  }
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

}
