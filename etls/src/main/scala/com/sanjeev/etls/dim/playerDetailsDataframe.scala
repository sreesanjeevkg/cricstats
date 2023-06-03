package com.sanjeev.etls.dim

import com.sanjeev.utilities.TableInfo
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.sanjeev.model.playerInfo
import com.sanjeev.utilities.loader.readData

/**
 * @author ${user.name}
 */
object playerDetailsDataframe extends TableInfo[playerInfo] {
  override def createDataFrame(sparkSession: SparkSession, loadedDependencies: Map[String, DataFrame]): DataFrame = {

    import sparkSession.implicits._

    val cricData = readData("/Users/sreesanjeev/Projects/cricstats/etls/src/main/resources/Players.csv", sparkSession)

    cricData.as("cd")
      .withColumn("Name", when($"FULL NAME" isNotNull, $"FULL NAME").otherwise($"Full Name"))
      .withColumn("Role", when($"PLAYING ROLE" isNotNull, $"PLAYING ROLE").otherwise($"Playing Role"))
      .withColumn("BattingStyle", when($"BATTING STYLE" isNotNull, $"BATTING STYLE").otherwise($"Batting Style"))
      .withColumn("BowlingStyle", when($"BOWLING STYLE" isNotNull, $"BOWLING STYLE").otherwise($"Bowling Style"))
      .select($"player_identifier", $"Name", $"Role", $"BattingStyle", $"BowlingStyle", $"FIELDING POSITION".as("FieldingPosition"))
      .as[playerInfo]
      .toDF()

  }

}
