package com.netease.arctic.spark.sql.execution

import com.netease.arctic.spark.source.{ArcticSource, SupportsDynamicOverwrite}
import org.apache.spark.sql.arctic.AnalysisException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class CreateArcticTableAsSelectCommand(arctic: ArcticSource,
                                            table: CatalogTable,
                                            mode: SaveMode,
                                            query: LogicalPlan,
                                            outputColumnNames: Seq[String])
  extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)
    val spark = SparkSession.getActiveSession.get
    val sparkCatalogImpl = spark.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION.key)
    if (!"hive".equalsIgnoreCase(sparkCatalogImpl)) {
      throw AnalysisException.message(s"failed to create table ${table.identifier} not use hive catalog")
    }
    // create table
    val arcticTable = arctic.createTable(table.identifier, table.schema,
      scala.collection.JavaConversions.seqAsJavaList(table.partitionColumnNames),
      scala.collection.JavaConversions.mapAsJavaMap(table.properties))

    // insert overwrite
    val mode = SaveMode.Overwrite
    val optWriter = arcticTable.createWriter("", query.schema, mode, null)
    if(!optWriter.isPresent){
      throw AnalysisException.message(s"failed to create writer for table ${table.identifier}")
    }
    val writer = optWriter.get match {
      case w: SupportsDynamicOverwrite =>
        w.overwriteDynamicPartitions()
    }
    WriteToDataSourceV2(writer, query)
    Seq.empty[Row]
  }

}
