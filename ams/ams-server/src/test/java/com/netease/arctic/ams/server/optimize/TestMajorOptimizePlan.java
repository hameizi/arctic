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

package com.netease.arctic.ams.server.optimize;


import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.ams.server.util.DataFileInfoUtils;
import com.netease.arctic.table.TableProperties;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.AdaptHiveGenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class TestMajorOptimizePlan extends TestBaseOptimizeBase {
  @Test
  public void testKeyedTableMajorOptimize() throws IOException {
    insertBasePosDeleteFiles(testKeyedTable, 2, baseDataFilesInfo, posDeleteFilesInfo);

    MajorOptimizePlan majorOptimizePlan = new MajorOptimizePlan(testKeyedTable,
        new TableOptimizeRuntime(testKeyedTable.id()), baseDataFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = majorOptimizePlan.plan();

    Assert.assertEquals(OptimizeType.Major, tasks.get(0).getTaskId().getType());
    Assert.assertEquals(4, tasks.size());
    Assert.assertEquals(10, tasks.get(0).getBaseFiles().size());
    Assert.assertEquals(1, tasks.get(0).getPosDeleteFiles().size());
    Assert.assertEquals(0, tasks.get(0).getInsertFileCnt());
    Assert.assertEquals(0, tasks.get(0).getDeleteFileCnt());
  }

  @Test
  public void testKeyedTableFullOptimize() throws IOException {
    insertBasePosDeleteFiles(testKeyedTable, 2, baseDataFilesInfo, posDeleteFilesInfo);

    testKeyedTable.updateProperties()
        .set(TableProperties.MAJOR_OPTIMIZE_TRIGGER_DELETE_FILE_SIZE_BYTES, "0")
        .commit();

    FullOptimizePlan fullOptimizePlan = new FullOptimizePlan(testKeyedTable,
        new TableOptimizeRuntime(testKeyedTable.id()), baseDataFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = fullOptimizePlan.plan();

    Assert.assertEquals(OptimizeType.FullMajor, tasks.get(0).getTaskId().getType());
    Assert.assertEquals(4, tasks.size());
    Assert.assertEquals(10, tasks.get(0).getBaseFiles().size());
    Assert.assertEquals(1, tasks.get(0).getPosDeleteFiles().size());
    Assert.assertEquals(0, tasks.get(0).getInsertFileCnt());
    Assert.assertEquals(0, tasks.get(0).getDeleteFileCnt());
  }

  @Test
  public void testUnKeyedTableMajorOptimize() {
    insertUnKeyedTableDataFiles();

    MajorOptimizePlan majorOptimizePlan = new MajorOptimizePlan(testTable,
        new TableOptimizeRuntime(testTable.id()), baseDataFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = majorOptimizePlan.plan();

    Assert.assertEquals(OptimizeType.Major, tasks.get(0).getTaskId().getType());
    Assert.assertEquals(2, tasks.size());
    Assert.assertEquals(5, tasks.get(0).getBaseFileCnt());
    Assert.assertEquals(0, tasks.get(0).getPosDeleteFiles().size());
    Assert.assertEquals(0, tasks.get(0).getInsertFileCnt());
    Assert.assertEquals(0, tasks.get(0).getDeleteFileCnt());
  }

  @Test
  public void testUnKeyedTableFullOptimize() {
    testTable.updateProperties()
        .set(TableProperties.FULL_OPTIMIZE_TRIGGER_MAX_INTERVAL, "86400000")
        .commit();
    insertUnKeyedTableDataFiles();

    FullOptimizePlan fullOptimizePlan = new FullOptimizePlan(testTable,
        new TableOptimizeRuntime(testTable.id()), baseDataFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = fullOptimizePlan.plan();

    Assert.assertEquals(OptimizeType.FullMajor, tasks.get(0).getTaskId().getType());
    Assert.assertEquals(2, tasks.size());
    Assert.assertEquals(5, tasks.get(0).getBaseFileCnt());
    Assert.assertEquals(0, tasks.get(0).getPosDeleteFiles().size());
    Assert.assertEquals(0, tasks.get(0).getInsertFileCnt());
    Assert.assertEquals(0, tasks.get(0).getDeleteFileCnt());
  }

  @Test
  public void testNoPartitionTableMajorOptimize() throws IOException {
    insertBasePosDeleteFiles(testNoPartitionTable, 2, baseDataFilesInfo, posDeleteFilesInfo);

    MajorOptimizePlan majorOptimizePlan = new MajorOptimizePlan(testNoPartitionTable,
        new TableOptimizeRuntime(testNoPartitionTable.id()), baseDataFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = majorOptimizePlan.plan();

    Assert.assertEquals(OptimizeType.Major, tasks.get(0).getTaskId().getType());
    Assert.assertEquals(4, tasks.size());
    Assert.assertEquals(10, tasks.get(0).getBaseFiles().size());
    Assert.assertEquals(1, tasks.get(0).getPosDeleteFiles().size());
    Assert.assertEquals(0, tasks.get(0).getInsertFileCnt());
    Assert.assertEquals(0, tasks.get(0).getDeleteFileCnt());
  }

  @Test
  public void testNoPartitionTableFullOptimize() throws IOException {
    insertBasePosDeleteFiles(testNoPartitionTable, 2, baseDataFilesInfo, posDeleteFilesInfo);

    testNoPartitionTable.updateProperties()
        .set(TableProperties.MAJOR_OPTIMIZE_TRIGGER_DELETE_FILE_SIZE_BYTES, "0")
        .commit();

    FullOptimizePlan fullOptimizePlan = new FullOptimizePlan(testNoPartitionTable,
        new TableOptimizeRuntime(testNoPartitionTable.id()), baseDataFilesInfo, posDeleteFilesInfo,
        new HashMap<>(), 1, System.currentTimeMillis(), snapshotId -> true);
    List<BaseOptimizeTask> tasks = fullOptimizePlan.plan();

    Assert.assertEquals(OptimizeType.FullMajor, tasks.get(0).getTaskId().getType());
    Assert.assertEquals(4, tasks.size());
    Assert.assertEquals(10, tasks.get(0).getBaseFiles().size());
    Assert.assertEquals(1, tasks.get(0).getPosDeleteFiles().size());
    Assert.assertEquals(0, tasks.get(0).getInsertFileCnt());
    Assert.assertEquals(0, tasks.get(0).getDeleteFileCnt());
  }

  private void insertUnKeyedTableDataFiles() {
    List<DataFile> dataFiles = insertUnKeyedTableDataFile(FILE_A.partition(), LocalDateTime.of(2022, 1, 1, 12, 0, 0), 5);
    dataFiles.addAll(insertUnKeyedTableDataFile(FILE_B.partition(), LocalDateTime.of(2022, 1, 2, 12, 0, 0), 5));

    AppendFiles appendFiles = testTable.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();

    long commitTime = System.currentTimeMillis();
    baseDataFilesInfo = dataFiles.stream()
        .map(dataFile -> DataFileInfoUtils.convertToDatafileInfo(dataFile, commitTime, testTable))
        .collect(Collectors.toList());
  }

  private List<DataFile> insertUnKeyedTableDataFile(StructLike partitionData, LocalDateTime opTime, int count) {
    List<DataFile> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      AdaptHiveGenericAppenderFactory
          appenderFactory = new AdaptHiveGenericAppenderFactory(testTable.schema(), testTable.spec());
      FileFormat fileFormat = FileFormat.valueOf((testTable.properties().getOrDefault(TableProperties.BASE_FILE_FORMAT,
          TableProperties.BASE_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH)));
      OutputFileFactory outputFileFactory = OutputFileFactory
          .builderFor(testTable, 0, 0).format(fileFormat).build();
      EncryptedOutputFile outputFile = outputFileFactory.newOutputFile(partitionData);
      DataFile targetFile = testTable.io().doAs(() -> {
        DataWriter<Record> writer = appenderFactory
            .newDataWriter(outputFile, FileFormat.PARQUET, partitionData);
        for (Record record : baseRecords(1, 100, opTime)) {
          writer.add(record);
        }
        writer.close();
        return writer.toDataFile();
      });

      result.add(targetFile);
    }

    return result;
  }

  private List<Record> baseRecords(int start, int length, LocalDateTime opTime) {
    GenericRecord record = GenericRecord.create(TABLE_SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    for (int i = start; i < start + length; i++) {
      builder.add(record.copy(ImmutableMap.of("id", i, "name", "name" + i, "op_time", opTime)));
    }

    return builder.build();
  }
}
