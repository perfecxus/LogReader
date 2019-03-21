package com.sample.datasource.ngnix

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}

/**
  * Created by sinchan on 29/09/18.
  */
class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val pathParameter = parameters.get("path")
    val skipBadRecords = parameters.getOrElse("skipBadRecords","true")
    pathParameter match {
      case Some(path) => new LogDataSourceRelation(sqlContext, path, skipBadRecords)
      case None => throw new IllegalArgumentException("The path parameter cannot be empty!")
    }
  }


}
