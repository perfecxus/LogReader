package com.sample.datasource.ngnix

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by sinchan on 29/09/18.
  */
class LogDataSourceRelation(override val sqlContext: SQLContext, path: String,skipBadRecords:String) extends BaseRelation with TableScan with PrunedScan with Serializable{

  private val ddd = "\\d{1,3}"                      // at least 1 but not more than 3 times (possessive)
  private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  // like `123.456.7.89`
  private val client = "(\\S+)"                     // '\S' is 'non-whitespace character'
  private val user = "(\\S+)"
  private val dateTime = "(\\[.+?\\])"              // like `[21/Jul/2009:02:48:13 -0700]`
  private val request = "\"(.*?)\""                 // any number of any character, reluctant
  private val status = "(\\d{3})"
  private val bytes = "(\\S+)"                      // this can be a "-"
  private val referer = "\"(.*?)\""
  private val agent = "\"(.*?)\""
  private val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
  private val p = Pattern.compile(regex)

  override def schema: StructType = {
      // Ngnix log schema
      StructType(Array(
        StructField("clientIp", StringType, true),
        StructField("rfc1413ClientId", StringType, true),
        StructField("remoteUser", StringType, true),
        StructField("dateTime", StringType, true),
        StructField("request", StringType, true),
        StructField("httpStatusCode", StringType, true),
        StructField("bytesSent",StringType,true),
        StructField("referer",StringType,true),
        StructField("userAgent",StringType,true)))
  }

  override def buildScan(): RDD[Row] = {
    println("Tablescan method")
    buildScan(Array[String]())
    /*val initialRdd = sqlContext.sparkContext.textFile(path)
    if(skipBadRecords=="true"){
      buildRDDWithSkipBadRecords(initialRdd,Array[String]())
    }
    else{
      buildRDD(initialRdd,Array[String]())
    }*/
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val initialRdd = sqlContext.sparkContext.textFile(path)
    if(skipBadRecords=="true"){
      buildRDDWithSkipBadRecords(initialRdd,requiredColumns)
    }
    else{
      buildRDD(initialRdd,requiredColumns)
    }
  }

  private def buildRDD(initialRdd: RDD[String],requiredColumns: Array[String]):RDD[Row] = {
    initialRdd.map(line => {
      val matcher = p.matcher(line)
      matcher.find()
      createRow(matcher, requiredColumns)

    })
  }

  private def buildRDDWithSkipBadRecords(initialRdd: RDD[String],requiredColumns: Array[String]):RDD[Row] = {
    initialRdd.flatMap(line => {
      val matcher = p.matcher(line)
      if (matcher.find())
        Some(createRow(matcher, requiredColumns))
      else
        None
    }).filter(row => row != None)
  }

  private def createRow(matcher: Matcher, requiredColumns: Array[String] = null) ={
    if (requiredColumns.isEmpty || requiredColumns.length == 9) {
      Row.fromSeq(Seq(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4).replaceAll("[\\[\\]]", ""), matcher.group(5), matcher.group(6), matcher.group(7), matcher.group(8), matcher.group(9)))
    }
    else {
      val allCols = schema.toList.map(structField => structField.name)
      Row.fromSeq(requiredColumns.map(col => if (allCols.indexOf(col) == 3) matcher.group(allCols.indexOf(col) + 1).replaceAll("[\\[\\]]", "") else matcher.group(allCols.indexOf(col) + 1)).toSeq)

      //3 elements --> build
      //buildScan --> PrunedScan  --> requiredCols <=> schema [0,3,9]  ==> Row(valu0,value3,value9)

    }
  }
}
