package com.netease.arctic.spark.source;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public interface TableSupport {

  DataSourceTable createTable(TableIdentifier identifier,
                              StructType schema, List<String> partitions, Map<String, String> properties);


  DataSourceTable loadTable(TableIdentifier identifier);

  boolean tableExists(TableIdentifier tableIdentifier);

  boolean dropTable(TableIdentifier identifier, boolean purge);
}
