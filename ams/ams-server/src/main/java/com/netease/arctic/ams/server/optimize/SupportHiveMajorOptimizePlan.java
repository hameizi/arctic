package com.netease.arctic.ams.server.optimize;

import com.google.common.base.Preconditions;
import com.netease.arctic.ams.api.DataFileInfo;
import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.hive.utils.TableTypeUtil;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SupportHiveMajorOptimizePlan extends MajorOptimizePlan {
  private static final Logger LOG = LoggerFactory.getLogger(SupportHiveMajorOptimizePlan.class);

  // hive location.
  protected final String hiveLocation;
  // files in locations don't need to major optimize
  protected final Set<String> excludeLocations = new HashSet<>();

  public SupportHiveMajorOptimizePlan(ArcticTable arcticTable, TableOptimizeRuntime tableOptimizeRuntime,
                                      List<DataFileInfo> baseTableFileList, List<DataFileInfo> posDeleteFileList,
                                      Map<String, Boolean> partitionTaskRunning, int queueId, long currentTime,
                                      Predicate<Long> snapshotIsCached) {
    super(arcticTable, tableOptimizeRuntime, baseTableFileList, posDeleteFileList,
        partitionTaskRunning, queueId, currentTime, snapshotIsCached);

    Preconditions.checkArgument(TableTypeUtil.isHive(arcticTable), "The table not support hive");
    hiveLocation = ((SupportHive) arcticTable).hiveLocation();
    excludeLocations.add(hiveLocation);
  }

  @Override
  public boolean partitionNeedPlan(String partitionToPath) {
    long current = System.currentTimeMillis();

    // check small data file count
    List<DataFile> smallFiles = filterSmallFiles(partitionToPath,
        partitionNeedMajorOptimizeFiles.getOrDefault(partitionToPath, new ArrayList<>()));
    if (checkSmallFileCount(smallFiles)) {
      partitionOptimizeType.put(partitionToPath, OptimizeType.Major);
      return true;
    }

    // check major optimize interval
    if (checkMajorOptimizeInterval(current, partitionToPath)) {
      partitionOptimizeType.put(partitionToPath, OptimizeType.Major);
      return true;
    }

    LOG.debug("{} ==== don't need {} optimize plan, skip partition {}", tableId(), getOptimizeType(), partitionToPath);
    return false;
  }

  @Override
  protected boolean checkMajorOptimizeInterval(long current, String partitionToPath) {
    if (current - tableOptimizeRuntime.getLatestMajorOptimizeTime(partitionToPath) >=
        PropertyUtil.propertyAsLong(arcticTable.properties(), TableProperties.MAJOR_OPTIMIZE_TRIGGER_MAX_INTERVAL,
            TableProperties.MAJOR_OPTIMIZE_TRIGGER_MAX_INTERVAL_DEFAULT)) {
      // need to rewrite or move all files that not in hive location to hive location.
      long fileCount = partitionNeedMajorOptimizeFiles.get(partitionToPath) == null ?
          0 : partitionNeedMajorOptimizeFiles.get(partitionToPath).size();
      return fileCount >= 1;
    }

    return false;
  }

  @Override
  protected void fillPartitionNeedOptimizeFiles(String partition, ContentFile<?> contentFile) {
    // for support hive table, add all files in iceberg base store and not in hive store
    if (canInclude(contentFile.path().toString())) {
      List<DataFile> files = partitionNeedMajorOptimizeFiles.computeIfAbsent(partition, e -> new ArrayList<>());
      files.add((DataFile) contentFile);
      partitionNeedMajorOptimizeFiles.put(partition, files);
    }
  }

  @Override
  protected boolean needOptimize(List<DeleteFile> posDeleteFiles, List<DataFile> baseFiles) {
    boolean hasPos = CollectionUtils.isNotEmpty(posDeleteFiles) && baseFiles.size() >= 2;
    boolean noPos = CollectionUtils.isEmpty(posDeleteFiles) && CollectionUtils.isNotEmpty(baseFiles);
    return hasPos || noPos;
  }

  private List<DataFile> filterSmallFiles(String partition, List<DataFile> dataFileList) {
    // for support hive table, filter small files
    List<DataFile> smallFileList = dataFileList.stream().filter(file -> file.fileSizeInBytes() <=
        PropertyUtil.propertyAsLong(arcticTable.properties(), TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD,
            TableProperties.OPTIMIZE_SMALL_FILE_SIZE_BYTES_THRESHOLD_DEFAULT)).collect(Collectors.toList());

    // if iceberg store has pos-delete, only optimize small files
    if (CollectionUtils.isNotEmpty(partitionPosDeleteFiles.get(partition))) {
      partitionNeedMajorOptimizeFiles.put(partition, smallFileList);
    }

    return smallFileList;
  }

  private boolean canInclude(String filePath) {
    for (String exclude : excludeLocations) {
      if (filePath.contains(exclude)) {
        return false;
      }
    }
    return true;
  }
}
