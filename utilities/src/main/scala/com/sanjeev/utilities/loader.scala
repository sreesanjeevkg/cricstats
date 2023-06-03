package com.sanjeev.utilities

import com.sanjeev.etls.dim._
import org.apache.spark.sql._
import com.sanjeev.model.playerInfo

object loader extends Utilities {
    def main(args: Array[String]): Unit = {
        // Your code here
        val spark = createSparkSession("testing", false, false)

        spark.sql("set spark.sql.caseSensitive=true")

        // loadedDependencies

        val playerInfoData: DataFrame = playerDetailsDataframe.createDataFrame(spark, loadedDependencies = Map.empty)

        val bowlingStyleInfoData: DataFrame = bowlingStyleDataframe.createDataFrame(spark, loadedDependencies = Map("playerInfo" -> playerInfoData))

        playerInfoData.show()
        println(bowlingStyleInfoData.count())

//
//        playerInfoData
//          .write.format("parquet")
//          .option("compression", "gzip")
//          .save("/Users/sreesanjeev/Projects/cricstats/etls/src/main/resources/parquets/playerInfo")

    }
}
