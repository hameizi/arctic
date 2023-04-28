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

package com.netease.arctic.flink.lookup;

import com.netease.arctic.utils.map.RocksDBBackend;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class RocksDBSetMemoryState extends RocksDBState<Set<ByteArrayWrapper>> {
  protected BinaryRowDataSerializerWrapper keySerializer;

  public RocksDBSetMemoryState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      BinaryRowDataSerializerWrapper joinKeySerializer,
      BinaryRowDataSerializerWrapper uniqueKeySerialization,
      BinaryRowDataSerializerWrapper valueSerializer,
      MetricGroup metricGroup,
      LookupOptions lookupOptions) {
    super(
        rocksDB,
        columnFamilyName,
        uniqueKeySerialization,
        valueSerializer,
        metricGroup,
        lookupOptions,
        true);
    this.keySerializer = joinKeySerializer;
  }

  @Override
  public void flush() {
  }

  public void batchWrite(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    byte[] joinKeyBytes = serializeKey(joinKey);
    LookupRecord.OpType opType = convertToOpType(joinKey.getRowKind());
    putIntoQueue(LookupRecord.of(opType, joinKeyBytes, uniqueKeyBytes));
  }

  @Override
  public byte[] serializeKey(RowData key) throws IOException {
    return serializeKey(keySerializer, key);
  }

  public void merge(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    byte[] joinKeyBytes = serializeKey(joinKey);
    putSecondaryCache(joinKeyBytes, uniqueKeyBytes);
  }

  public void delete(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    final byte[] joinKeyBytes = serializeKey(joinKey);
    deleteSecondaryCache(joinKeyBytes, uniqueKeyBytes);
  }

  /**
   * Retrieve the elements of the key.
   * <p>Fetch the Collection from guava cache,
   * if not present, fetch from rocksDB continuously, via prefix key scanning the rocksDB;
   * if present, just return the result.
   *
   * @return not null, but may be empty.
   */
  public Collection<ByteArrayWrapper> get(RowData key) throws IOException {
    final byte[] keyBytes = serializeKey(key);
    ByteArrayWrapper keyWrap = wrap(keyBytes);
    Set<ByteArrayWrapper> result = guavaCache.getIfPresent(keyWrap);
    if (result == null) {
      return Collections.emptyList();
    }
    return result;
  }

  @Override
  public void putCacheValue(Cache<ByteArrayWrapper, Set<ByteArrayWrapper>> cache,
                            ByteArrayWrapper keyWrap, ByteArrayWrapper valueWrap) {
    guavaCache.asMap().compute(keyWrap, (keyWrapper, oldSet) -> {
      if (oldSet == null) {
        oldSet = Sets.newHashSet();
      }
      oldSet.add(valueWrap);
      return oldSet;
    });
  }

  @Override
  public void removeValue(
      Cache<ByteArrayWrapper, Set<ByteArrayWrapper>> cache, ByteArrayWrapper keyWrap, ByteArrayWrapper valueWrap) {
    guavaCache.asMap().compute(keyWrap, (keyWrapper, oldSet) -> {
      if (oldSet == null) {
        return null;
      }
      oldSet.remove(valueWrap);
      if (oldSet.isEmpty()) {
        return null;
      }
      return oldSet;
    });
  }
}
