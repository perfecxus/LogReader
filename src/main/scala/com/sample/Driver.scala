package com.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
  * Created by sinchan on 27/09/18.
  */
object Driver {

  def main(args: Array[String]): Unit = {

    if(args.length < 1) {
      println("please pass the log file location as thefirst argument")
      System.exit(-1)
    }

    val logPath = args(0)

    val spark = SparkSession.builder().appName("NgnixLogReader_App").getOrCreate()

    val loadedDf = spark.read.format("com.sample.datasource.ngnix").load(logPath) //using customized datasource api for ngnix log records //.option("skipBadRecords","false")
          //spark.read.format("csv").option("delimiter","|").load("")

    loadedDf.persist(StorageLevel.MEMORY_AND_DISK)
    //loadedDf.cache()

    loadedDf.show(false) //showing only first 20 values

   loadedDf.createOrReplaceTempView("loadedDf")


    val sortedIpFrequencyDf = spark.sql("select clientIp,count(1) as ipFrequency from loadedDf group by clientIp order by ipFrequency desc")
    sortedIpFrequencyDf.show(50) //showing only first 50 rows -
    // for all rows, use
    // sortedIpFrequencyDf.collect().foreach(println)


    val sortedStatusFrequencyDf = spark.sql("select httpStatusCode,count(1) as statusFrequency from loadedDf group by httpStatusCode order by statusFrequency desc")
    sortedStatusFrequencyDf.show()

  }

}
