package com.sanjeev.utilities

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe.TypeTag

abstract class TableInfo[T <: Product with Serializable : TypeTag] {

  def dependsOn: Set[String] = Set.empty[String]

  protected def createDataFrame(sparkSession: SparkSession, loadedDependencies: Map[String, DataFrame]): DataFrame = {
    import sparkSession.implicits._
    sparkSession.emptyDataset[T].toDF()
  }

}
