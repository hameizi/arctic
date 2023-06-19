package com.netease.arctic.server.dashboard;

import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.FileNameRules;
import com.netease.arctic.server.dashboard.model.AMSDataFileInfo;
import com.netease.arctic.server.dashboard.model.DDLInfo;
import com.netease.arctic.server.dashboard.model.FilesStatistics;
import com.netease.arctic.server.dashboard.model.OptimizedRecord;
import com.netease.arctic.server.dashboard.model.OptimizingTaskStat;
import com.netease.arctic.server.dashboard.model.PartitionBaseInfo;
import com.netease.arctic.server.dashboard.model.PartitionFileBaseInfo;
import com.netease.arctic.server.dashboard.model.TableOptimizingProcess;
import com.netease.arctic.server.dashboard.model.TransactionsOfTable;
import com.netease.arctic.server.dashboard.response.PageResult;
import com.netease.arctic.server.optimizing.MetricsSummary;
import com.netease.arctic.server.optimizing.TaskRuntime;
import com.netease.arctic.server.persistence.PersistentBase;
import com.netease.arctic.server.persistence.mapper.OptimizingMapper;
import com.netease.arctic.server.persistence.mapper.TableMetaMapper;
import com.netease.arctic.server.table.ServerTableIdentifier;
import com.netease.arctic.server.table.TableService;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.trace.SnapshotSummary;
import com.netease.arctic.utils.ManifestEntryFields;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServerTableDescriptor extends PersistentBase {

  private static final Logger LOG = LoggerFactory.getLogger(ServerTableDescriptor.class);
  private static final Map<Integer, DataFileType> ICEBERG_FILE_TYPE_MAP = new HashMap<>();

  private final TableService tableService;

  static {
    ICEBERG_FILE_TYPE_MAP.put(FileContent.DATA.id(), DataFileType.BASE_FILE);
    ICEBERG_FILE_TYPE_MAP.put(FileContent.POSITION_DELETES.id(), DataFileType.POS_DELETE_FILE);
    ICEBERG_FILE_TYPE_MAP.put(FileContent.EQUALITY_DELETES.id(), DataFileType.EQ_DELETE_FILE);
  }

  public ServerTableDescriptor(TableService tableService) {
    this.tableService = tableService;
  }

  private ServerTableIdentifier getTable(String catalog, String db, String table) {
    return getAs(TableMetaMapper.class, mapper -> mapper.selectTableIdentifier(catalog, db, table));
  }

  public List<TransactionsOfTable> getTransactions(ServerTableIdentifier tableIdentifier) {
    List<TransactionsOfTable> transactionsOfTables = new ArrayList<>();
    ArcticTable arcticTable = tableService.loadTable(tableIdentifier);
    List<Table> tables = new ArrayList<>();
    if (arcticTable.isKeyedTable()) {
      tables.add(arcticTable.asKeyedTable().changeTable());
      tables.add(arcticTable.asKeyedTable().baseTable());
    } else {
      tables.add(arcticTable.asUnkeyedTable());
    }
    tables.forEach(table -> table.snapshots().forEach(snapshot -> {
      if (snapshot.operation().equals(DataOperations.REPLACE)) {
        return;
      }
      if (snapshot.summary().containsKey(SnapshotSummary.TRANSACTION_BEGIN_SIGNATURE)) {
        return;
      }
      TransactionsOfTable transactionsOfTable = new TransactionsOfTable();
      transactionsOfTable.setTransactionId(snapshot.snapshotId());
      int fileCount = PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP, 0);
      fileCount += PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP, 0);
      fileCount += PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP, 0);
      fileCount += PropertyUtil
          .propertyAsInt(snapshot.summary(), org.apache.iceberg.SnapshotSummary.REMOVED_DELETE_FILES_PROP, 0);
      transactionsOfTable.setFileCount(fileCount);
      transactionsOfTable.setFileSize(PropertyUtil
          .propertyAsLong(snapshot.summary(), org.apache.iceberg.SnapshotSummary.ADDED_FILE_SIZE_PROP, 0) +
          PropertyUtil
              .propertyAsLong(snapshot.summary(), org.apache.iceberg.SnapshotSummary.REMOVED_FILE_SIZE_PROP, 0));
      transactionsOfTable.setCommitTime(snapshot.timestampMillis());
      transactionsOfTables.add(transactionsOfTable);
    }));
    transactionsOfTables.sort((o1, o2) -> Long.compare(o2.commitTime, o1.commitTime));
    return transactionsOfTables;
  }

  public List<AMSDataFileInfo> getTransactionDetail(ServerTableIdentifier tableIdentifier, long transactionId) {
    List<AMSDataFileInfo> result = new ArrayList<>();
    ArcticTable arcticTable = tableService.loadTable(tableIdentifier);
    Snapshot snapshot;
    if (arcticTable.isKeyedTable()) {
      snapshot = arcticTable.asKeyedTable().changeTable().snapshot(transactionId);
      if (snapshot == null) {
        snapshot = arcticTable.asKeyedTable().baseTable().snapshot(transactionId);
      }
    } else {
      snapshot = arcticTable.asUnkeyedTable().snapshot(transactionId);
    }
    if (snapshot == null) {
      throw new IllegalArgumentException("unknown snapshot " + transactionId + " of " + tableIdentifier);
    }
    final long snapshotTime = snapshot.timestampMillis();
    snapshot.addedDataFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "add"));
    });
    snapshot.removedDataFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "remove"));
    });
    snapshot.addedDeleteFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "add"));
    });
    snapshot.removedDeleteFiles(arcticTable.io()).forEach(f -> {
      result.add(new AMSDataFileInfo(
          f.path().toString(),
          arcticTable.spec(), f.partition(),
          f.content(),
          f.fileSizeInBytes(),
          snapshotTime,
          "remove"));
    });
    return result;
  }

  public List<DDLInfo> getTableOperations(ServerTableIdentifier tableIdentifier) {
    List<DDLInfo> result = new ArrayList<>();
    ArcticTable arcticTable = tableService.loadTable(tableIdentifier);
    Table table;
    if (arcticTable.isKeyedTable()) {
      table = arcticTable.asKeyedTable().baseTable();
    } else {
      table = arcticTable.asUnkeyedTable();
    }
    List<HistoryEntry> snapshotLog = ((HasTableOperations) table).operations().current().snapshotLog();
    List<org.apache.iceberg.TableMetadata.MetadataLogEntry> metadataLogEntries =
        ((HasTableOperations) table).operations().current().previousFiles();
    Set<Long> time = new HashSet<>();
    snapshotLog.forEach(e -> time.add(e.timestampMillis()));
    for (int i = 1; i < metadataLogEntries.size(); i++) {
      org.apache.iceberg.TableMetadata.MetadataLogEntry e = metadataLogEntries.get(i);
      if (!time.contains(e.timestampMillis())) {
        org.apache.iceberg.TableMetadata
            oldTableMetadata = TableMetadataParser.read(table.io(), metadataLogEntries.get(i - 1).file());
        org.apache.iceberg.TableMetadata
            newTableMetadata = TableMetadataParser.read(table.io(), metadataLogEntries.get(i).file());
        DDLInfo.Generator generator = new DDLInfo.Generator();
        result.addAll(generator.tableIdentify(arcticTable.id())
            .oldMeta(oldTableMetadata)
            .newMeta(newTableMetadata)
            .generate());
      }
    }
    if (metadataLogEntries.size() > 0) {
      org.apache.iceberg.TableMetadata oldTableMetadata = TableMetadataParser.read(
          table.io(),
          metadataLogEntries.get(metadataLogEntries.size() - 1).file());
      org.apache.iceberg.TableMetadata newTableMetadata = ((HasTableOperations) table).operations().current();
      DDLInfo.Generator generator = new DDLInfo.Generator();
      result.addAll(generator.tableIdentify(arcticTable.id())
          .oldMeta(oldTableMetadata)
          .newMeta(newTableMetadata)
          .generate());
    }
    return result;
  }

  @Deprecated
  public List<OptimizedRecord> getOptimizeInfo(String catalog, String db, String table) {
    List<TableOptimizingProcess> tableOptimizingProcesses = getAs(
        OptimizingMapper.class,
        mapper -> mapper.selectSuccessOptimizingProcesses(catalog, db, table));
    return tableOptimizingProcesses.stream().map(optimizingProcess -> {
      OptimizedRecord record = new OptimizedRecord();
      record.setCommitTime(optimizingProcess.getFinishTime());
      record.setPlanTime(optimizingProcess.getStartTime());
      record.setDuration(optimizingProcess.getFinishTime() - optimizingProcess.getStartTime());
      record.setTableIdentifier(TableIdentifier.of(optimizingProcess.getCatalogName(), optimizingProcess.getDbName(),
          optimizingProcess.getTableName()));
      record.setOptimizeType(optimizingProcess.getOptimizingType());
      MetricsSummary metricsSummary = optimizingProcess.getSummary();
      record.setTotalFilesStatBeforeCompact(FilesStatistics.builder()
          .addFiles(metricsSummary.getEqualityDeleteSize(), metricsSummary.getEqDeleteFileCnt())
          .addFiles(metricsSummary.getPositionalDeleteSize(), metricsSummary.getPosDeleteFileCnt())
          .addFiles(metricsSummary.getRewriteDataSize(), metricsSummary.getRewriteDataFileCnt())
          .build());
      record.setTotalFilesStatAfterCompact(FilesStatistics.build(
          metricsSummary.getNewFileCnt(),
          metricsSummary.getNewFileSize()));
      return record;
    }).collect(Collectors.toList());
  }

  public PageResult<TableOptimizingProcess> getOptimizingProcesses(String catalog, String db, String table, int offset,
                                                                   int limit) {
    List<TableOptimizingProcess> tableOptimizingProcesses = getAs(
        OptimizingMapper.class,
        mapper -> mapper.selectOptimizingProcesses(catalog, db, table, offset, limit));
    List<Long> processIds =
        tableOptimizingProcesses.stream().map(TableOptimizingProcess::getProcessId).collect(Collectors.toList());
    if (!processIds.isEmpty()) {
      List<OptimizingTaskStat> optimizingTaskStats = getAs(OptimizingMapper.class,
          mapper -> mapper.selectOptimizeTaskStats(processIds));
      initTaskStats(tableOptimizingProcesses, optimizingTaskStats);
    }
    Integer total = getAs(
        OptimizingMapper.class,
        mapper -> mapper.selectCountOptimizingProcesses(catalog, db, table));
    tableOptimizingProcesses.forEach(TableOptimizingProcess::init);
    return PageResult.of(tableOptimizingProcesses, total);
  }

  private void initTaskStats(List<TableOptimizingProcess> tableOptimizingProcesses,
                             List<OptimizingTaskStat> optimizingTaskStats) {
    Map<Long, Map<TaskRuntime.Status, Integer>> taskCountGroupByStatus = Maps.newHashMap();
    for (OptimizingTaskStat stat : optimizingTaskStats) {
      taskCountGroupByStatus
          .computeIfAbsent(stat.getProcessId(), p -> Maps.newHashMap())
          .put(stat.getStatus(), stat.getCount());
    }
    tableOptimizingProcesses.forEach(
        p -> initProcessTaskStats(p, taskCountGroupByStatus.get(p.getProcessId())));
  }

  private void initProcessTaskStats(TableOptimizingProcess process, Map<TaskRuntime.Status, Integer> status2TaskCount) {
    if (status2TaskCount == null) {
      return;
    }
    int totalTasksCount = 0;
    int successTasksCount = 0;
    int runningTasksCount = 0;
    for (Map.Entry<TaskRuntime.Status, Integer> entry : status2TaskCount.entrySet()) {
      Integer taskCount = entry.getValue();
      totalTasksCount += taskCount;
      switch (entry.getKey()) {
        case SUCCESS:
          successTasksCount += taskCount;
          break;
        case SCHEDULED:
        case ACKED:
          runningTasksCount += taskCount;
      }
    }
    process.setRunningTasks(runningTasksCount);
    process.setSuccessTasks(successTasksCount);
    process.setTotalTasks(totalTasksCount);
  }

  public List<PartitionBaseInfo> getTablePartition(ArcticTable arcticTable) {
    if (arcticTable.spec().isUnpartitioned()) {
      return new ArrayList<>();
    }
    Map<String, PartitionBaseInfo> partitionBaseInfoHashMap = new HashMap<>();
    getTableFile(arcticTable, null, Integer.MAX_VALUE).forEach(fileInfo -> {
      if (!partitionBaseInfoHashMap.containsKey(fileInfo.getPartitionName())) {
        partitionBaseInfoHashMap.put(fileInfo.getPartitionName(), new PartitionBaseInfo());
        partitionBaseInfoHashMap.get(fileInfo.getPartitionName()).setPartition(fileInfo.getPartitionName());
      }
      PartitionBaseInfo partitionInfo = partitionBaseInfoHashMap.get(fileInfo.getPartitionName());
      partitionInfo.setFileCount(partitionInfo.getFileCount() + 1);
      partitionInfo.setFileSize(partitionInfo.getFileSize() + fileInfo.getFileSize());
      partitionInfo.setLastCommitTime(partitionInfo.getLastCommitTime() > fileInfo.getCommitTime() ?
          partitionInfo.getLastCommitTime() :
          fileInfo.getCommitTime());
    });

    return new ArrayList<>(partitionBaseInfoHashMap.values());
  }

  public List<PartitionFileBaseInfo> getTableFile(ArcticTable arcticTable, String partition, int limit) {
    List<PartitionFileBaseInfo> result = new ArrayList<>();
    if (arcticTable.isKeyedTable()) {
      result.addAll(collectFileInfo(arcticTable.asKeyedTable().changeTable(), true, partition, limit));
      result.addAll(collectFileInfo(arcticTable.asKeyedTable().baseTable(), false, partition, limit));
    } else {
      result.addAll(collectFileInfo(arcticTable.asUnkeyedTable(), false, partition, limit));
    }
    return result;
  }

  private List<PartitionFileBaseInfo> collectFileInfo(Table table, boolean isChangeTable, String partition, int limit) {
    PartitionSpec spec = table.spec();
    List<PartitionFileBaseInfo> result = new ArrayList<>();
    Table entriesTable = MetadataTableUtils.createMetadataTableInstance(((HasTableOperations) table).operations(),
        table.name(), table.name() + "#ENTRIES",
        MetadataTableType.ENTRIES);
    try (CloseableIterable<Record> manifests = IcebergGenerics.read(entriesTable)
        .where(Expressions.notEqual(ManifestEntryFields.STATUS.name(), ManifestEntryFields.Status.DELETED.id()))
        .build()) {
      for (Record record : manifests) {
        long snapshotId = (long) record.getField(ManifestEntryFields.SNAPSHOT_ID.name());
        GenericRecord dataFile = (GenericRecord) record.getField(ManifestEntryFields.DATA_FILE_FIELD_NAME);
        Integer contentId = (Integer) dataFile.getField(DataFile.CONTENT.name());
        String filePath = (String) dataFile.getField(DataFile.FILE_PATH.name());
        String partitionPath = null;
        GenericRecord parRecord = (GenericRecord) dataFile.getField(DataFile.PARTITION_NAME);
        if (parRecord != null) {
          InternalRecordWrapper wrapper = new InternalRecordWrapper(parRecord.struct());
          partitionPath = spec.partitionToPath(wrapper.wrap(parRecord));
        }
        if (partition != null && spec.isPartitioned() && !partition.equals(partitionPath)) {
          continue;
        }
        Long fileSize = (Long) dataFile.getField(DataFile.FILE_SIZE.name());
        DataFileType dataFileType =
            isChangeTable ? FileNameRules.parseFileTypeForChange(filePath) : ICEBERG_FILE_TYPE_MAP.get(contentId);
        long commitTime = -1;
        if (table.snapshot(snapshotId) != null) {
          commitTime = table.snapshot(snapshotId).timestampMillis();
        }
        result.add(new PartitionFileBaseInfo(snapshotId, dataFileType, commitTime,
            partitionPath, filePath, fileSize));
        if (result.size() >= limit) {
          return result;
        }
      }
    } catch (IOException exception) {
      LOG.error("close manifest file error", exception);
    }
    return result;
  }
}
