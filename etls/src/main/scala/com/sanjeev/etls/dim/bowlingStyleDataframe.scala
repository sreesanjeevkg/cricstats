package com.sanjeev.etls.dim

import com.sanjeev.utilities.TableInfo
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.sanjeev.model.bowlingStyleInfo
import org.apache.spark.sql.expressions.Window

object bowlingStyleDataframe extends TableInfo[bowlingStyleInfo] {

  //override val dependsOn = Set("playerDetailsDataframe")

  override def createDataFrame(sparkSession: SparkSession, loadedDependencies: Map[String, DataFrame]): DataFrame = {

    import sparkSession.implicits._

    val cricData = loadedDependencies("playerInfo")

    cricData.as("cd")
      .select("BowlingStyle")
      .distinct()
      .withColumn("bowlingStyleID", row_number().over(Window.orderBy($"BowlingStyle")).cast("Int"))
      .as[bowlingStyleInfo]
      .toDF()

  }

}
