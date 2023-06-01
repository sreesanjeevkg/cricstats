package com.sanjeev.etls.dim

import com.sanjeev.utilities.Utilities
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.sanjeev.model.playerInfo

/**
 * @author ${user.name}
 */
object playerDetailsDataframe {
  def createDataframe(cricData: DataFrame, spark: SparkSession): Dataset[playerInfo] = {

    import spark.implicits._

    cricData.as("cd")
      .withColumn("Name", when($"FULL NAME" isNotNull, $"FULL NAME").otherwise($"Full Name"))
      .withColumn("Role", when($"PLAYING ROLE" isNotNull, $"PLAYING ROLE").otherwise($"Playing Role"))
      .withColumn("BattingStyle", when($"BATTING STYLE" isNotNull, $"BATTING STYLE").otherwise($"Batting Style"))
      .withColumn("BowlingStyle", when($"BOWLING STYLE" isNotNull, $"BOWLING STYLE").otherwise($"Bowling Style"))
      .select($"player_identifier", $"Name", $"Role", $"BattingStyle", $"BowlingStyle", $"FIELDING POSITION".as("FieldingPosition"))
      .as[playerInfo]

  }

}
