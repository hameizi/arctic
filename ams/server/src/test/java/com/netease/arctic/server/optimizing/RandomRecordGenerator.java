/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.server.optimizing;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class RandomRecordGenerator {

  private Random random = new Random();

  private Schema primary;

  private Set<Integer> primaryIds = new HashSet<>();

  private Map<Integer, Object>[] partitionValues;

  private Map<Integer, Map<Integer, Object>> primaryRelationWithPartition = new HashMap<>();

  private Set<Integer> partitionIds = new HashSet<>();

  private Schema schema;

  public RandomRecordGenerator(Schema schema, PartitionSpec spec, Schema primary, int partitionCount) {
    this.schema = schema;
    this.primary = primary;
    if (this.primary != null) {
      for (Types.NestedField field : primary.columns()) {
        Preconditions.checkState(field.type().typeId() == Type.TypeID.INTEGER, "primary key must be int type");
        primaryIds.add(field.fieldId());
      }
    }

    if (!spec.isUnpartitioned()) {
      partitionValues = new Map[partitionCount];
      for (int i = 0; i < partitionCount; i++) {
        partitionValues[i] = new HashMap<>();
        for (PartitionField field : spec.fields()) {
          partitionIds.add(field.sourceId());
          partitionValues[i].put(field.sourceId(), generateObject(schema.findType(field.sourceId())));
        }
      }
    }
  }

  public List<Record> range(int lowerBound, int upperBound) {
    Preconditions.checkNotNull(primary, "This method is only for primary table");
    Preconditions.checkState(lowerBound <= upperBound);
    List<Record> list = new ArrayList<>();
    for (int i = lowerBound; i <= upperBound; i++) {
      list.add(randomRecord(i));
    }
    return list;
  }

  public List<Record> scatter(int[] primaries) {
    Preconditions.checkNotNull(primary, "This method is only for primary table");
    Preconditions.checkNotNull(primaries);
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < primaries.length; i++) {
      list.add(randomRecord(primaries[i]));
    }
    return list;
  }

  private Record randomRecord(int primaryValue) {
    Record record = GenericRecord.create(schema);
    Random random = new Random();
    List<Types.NestedField> columns = schema.columns();
    Map<Integer, Object> partitionValue = null;
    if (partitionValues != null) {
      partitionValue =
          primaryRelationWithPartition.computeIfAbsent(
              primaryValue,
              p -> partitionValues[random.nextInt(partitionValues.length)]);
    }
    for (int i = 0; i < columns.size(); i++) {
      Types.NestedField field = columns.get(i);

      if (primaryIds.contains(field.fieldId())) {
        record.set(i, primaryValue);
        continue;
      }

      if (partitionIds.contains(field.fieldId())) {
        record.set(i, partitionValue.get(field.fieldId()));
        continue;
      }

      record.set(i, generateObject(field.type()));
    }
    return record;
  }

  private Object generateObject(Type type) {
    switch (type.typeId()) {
      case INTEGER:
        return random.nextInt();
      case STRING:
        return UUID.randomUUID().toString();
      case LONG:
        return random.nextLong();
      case FLOAT:
        return random.nextFloat();
      case DOUBLE:
        return random.nextDouble();
      case BOOLEAN:
        return random.nextBoolean();
      case DATE:
        return LocalDate.now().minusDays(random.nextInt(10000));
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        if (timestampType.shouldAdjustToUTC()) {
          return OffsetDateTime.now().minusDays(random.nextInt(10000));
        } else {
          return LocalDateTime.now().minusDays(random.nextInt(10000));
        }
      default:
        throw new RuntimeException("Unsupported type you can add them in code");
    }
  }
}
