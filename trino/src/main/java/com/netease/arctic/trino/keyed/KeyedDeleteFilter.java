/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.trino.keyed;

import com.netease.arctic.io.reader.ArcticDeleteFilter;
import com.netease.arctic.scan.KeyedTableScanTask;
import com.netease.arctic.table.PrimaryKeySpec;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.plugin.iceberg.delete.TrinoRow;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * KeyedDeleteFilter is used to do MOR for Keyed Table
 */
public class KeyedDeleteFilter extends ArcticDeleteFilter<TrinoRow> {

  private FileIO fileIO;

  protected KeyedDeleteFilter(
      KeyedTableScanTask keyedTableScanTask,
      Schema tableSchema,
      List<IcebergColumnHandle> requestedSchema,
      PrimaryKeySpec primaryKeySpec,
      FileIO fileIO) {
    super(keyedTableScanTask, tableSchema, getSchemas(requestedSchema), primaryKeySpec);
    this.fileIO = fileIO;
  }

  private static Schema getSchemas(List<IcebergColumnHandle> requestedColumns) {
    return new Schema(requestedColumns.stream().map(s -> Types.NestedField.optional(s.getId(), s.getName(),
        TypeConverter.toIcebergType(s.getType()))).collect(Collectors.toList()));
  }

  @Override
  protected StructLike asStructLike(TrinoRow record) {
    return record;
  }

  @Override
  protected InputFile getInputFile(String location) {
    return fileIO.newInputFile(location);
  }

  private static Schema filterSchema(Schema tableSchema, List<IcebergColumnHandle> requestedColumns) {
    return new Schema(filterFieldList(tableSchema.columns(), requestedColumns));
  }

  private static List<Types.NestedField> filterFieldList(List<Types.NestedField> fields,
      List<IcebergColumnHandle> requestedColumns) {
    return requestedColumns.stream().map(c -> filterField(c, fields).get())
        .collect(Collectors.toList());
  }

  private static List<Types.NestedField> filterFieldList(
      List<Types.NestedField> fields,
      Set<Integer> requestedFieldIds) {
    return fields.stream()
        .map(field -> filterField(field, requestedFieldIds))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toImmutableList());
  }

  private static Optional<Types.NestedField> filterField(IcebergColumnHandle requestColumn,
      List<Types.NestedField> fields) {
    for (Types.NestedField nestedField: fields) {
      if (nestedField.fieldId() == requestColumn.getId()) {
        return Optional.of(nestedField);
      }
      if (nestedField.type().isStructType()) {
        Optional<Types.NestedField> childField = filterField(requestColumn, nestedField.type().asStructType().fields());
        if (childField.isEmpty()) {
          return Optional.empty();
        }
        return childField;
      }
    }
    throw new RuntimeException("can not found columns " + requestColumn);
  }

  private static Optional<Types.NestedField> filterField(Types.NestedField field, Set<Integer> requestedFieldIds) {
    Type fieldType = field.type();
    if (requestedFieldIds.contains(field.fieldId())) {
      return Optional.of(field);
    }

    if (fieldType.isStructType()) {
      List<Types.NestedField> requiredChildren = filterFieldList(fieldType.asStructType().fields(), requestedFieldIds);
      if (requiredChildren.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(Types.NestedField.of(
          field.fieldId(),
          field.isOptional(),
          field.name(),
          Types.StructType.of(requiredChildren),
          field.doc()));
    }

    return Optional.empty();
  }
}
