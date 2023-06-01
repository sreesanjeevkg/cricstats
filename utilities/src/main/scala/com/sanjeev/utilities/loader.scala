package com.sanjeev.utilities

import com.sanjeev.etls.dim.playerDetailsDataframe
import org.apache.spark.sql._
import com.sanjeev.model.playerInfo

object loader extends Utilities {
    def main(args: Array[String]): Unit = {
        // Your code here
        val spark = createSparkSession("testing", false, false)

        spark.sql("set spark.sql.caseSensitive=true")

        val cricData = readData("/Users/sreesanjeev/Projects/cricstats/etls/src/main/resources/Players.csv", spark)

        val playerInfoData: Dataset[playerInfo] = playerDetailsDataframe.createDataframe(cricData, spark)

        playerInfoData.show()






    }
}
