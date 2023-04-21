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
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class RocksDBSetSpillState extends RocksDBState<Set<byte[]>> {
  protected BinaryRowDataSerializerWrapper keySerializer;

  public RocksDBSetSpillState(
      RocksDBBackend rocksDB,
      String columnFamilyName,
      long lruMaximumSize,
      BinaryRowDataSerializerWrapper joinKeySerializer,
      BinaryRowDataSerializerWrapper uniqueKeySerialization,
      BinaryRowDataSerializerWrapper valueSerializer,
      int writeRocksDBThreadNum) {
    super(
        rocksDB,
        columnFamilyName,
        lruMaximumSize,
        uniqueKeySerialization,
        valueSerializer,
        writeRocksDBThreadNum,
        true);
    this.keySerializer = joinKeySerializer;
  }

  @Override
  public void flush() {
  }

  public void batchWrite(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    byte[] joinKeyBytes = serializeKey(joinKey);
    RocksDBRecord.OpType opType = convertToOpType(joinKey.getRowKind());
    putIntoQueue(RocksDBRecord.of(opType, joinKeyBytes, uniqueKeyBytes));
  }

  @Override
  public byte[] serializeKey(RowData key) throws IOException {
    return serializeKey(keySerializer, key);
  }

  public void merge(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    byte[] joinKeyBytes = serializeKey(joinKey);
    ByteArrayWrapper keyWrap = wrap(joinKeyBytes);
    if (guavaCache.getIfPresent(keyWrap) != null) {
      guavaCache.invalidate(keyWrap);
    }
    putSecondaryMap(joinKeyBytes, uniqueKeyBytes);
  }

  public void delete(RowData joinKey, byte[] uniqueKeyBytes) throws IOException {
    final byte[] joinKeyBytes = serializeKey(joinKey);
    ByteArrayWrapper keyWrap = wrap(joinKeyBytes);
    if (guavaCache.getIfPresent(keyWrap) != null) {
      guavaCache.invalidate(keyWrap);
    }
    deleteSecondaryMap(joinKeyBytes, uniqueKeyBytes);
  }

  /**
   * Retrieve the elements of the key.
   * <p>Fetch the Collection from guava cache,
   * if not present, fetch from rocksDB continuously, via prefix key scanning the rocksDB;
   * if present, just return the result.
   *
   * @return not null, but may be empty.
   */
  public Collection<byte[]> get(RowData key) throws IOException {
    final byte[] keyBytes = serializeKey(key);
    ByteArrayWrapper keyWrap = wrap(keyBytes);
    Set<byte[]> result = guavaCache.getIfPresent(keyWrap);
    if (result == null) {
      Set<ByteArrayWrapper> otherKeys = secondaryIndexMap.get(keyWrap);
      if (otherKeys == null) {
        return Collections.emptyList();
      }
      result = otherKeys.stream().map(baw -> baw.bytes).collect(Collectors.toSet());
      guavaCache.put(keyWrap, result);
      return otherKeys.stream().map(wrap -> wrap.bytes).collect(Collectors.toList());
    }
    return result;
  }
}
