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

package com.netease.arctic.flink.table;

import com.netease.arctic.flink.InternalCatalogBuilder;
import com.netease.arctic.flink.catalog.ArcticCatalog;
import com.netease.arctic.flink.read.FlinkKafkaConsumerBase;
import com.netease.arctic.flink.table.descriptors.ArcticValidator;
import com.netease.arctic.flink.util.ArcticUtils;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.netease.arctic.flink.catalog.descriptors.ArcticCatalogValidator.METASTORE_URL;
import static com.netease.arctic.flink.catalog.descriptors.ArcticCatalogValidator.METASTORE_URL_OPTION;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableSourceOptions;

/**
 * A factory generates {@link ArcticDynamicSource} and {@link ArcticDynamicSink}
 */
public class DynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicTableFactory.class);
  public static final String IDENTIFIER = "arctic";
  private ArcticCatalog arcticCatalog;
  private InternalCatalogBuilder internalCatalogBuilder;
  private String internalCatalogName;

  public DynamicTableFactory(ArcticCatalog arcticCatalog,
                             InternalCatalogBuilder internalCatalogBuilder,
                             String internalCatalogName) {
    this.arcticCatalog = arcticCatalog;
    this.internalCatalogBuilder = internalCatalogBuilder;
    this.internalCatalogName = internalCatalogName;
  }

  public DynamicTableFactory() {
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    CatalogTable catalogTable = context.getCatalogTable();
    ObjectIdentifier identifier = context.getObjectIdentifier();
    ObjectPath objectPath;

    Map<String, String> options = catalogTable.getOptions();

    InternalCatalogBuilder actualBuilder = internalCatalogBuilder;
    String actualCatalogName = internalCatalogName;

    // It denotes create table by ddl 'connector' option, not through arcticCatalog.db.tableName
    if (actualBuilder == null || actualCatalogName == null) {
      String metastoreUrl = options.get(METASTORE_URL);
      Preconditions.checkNotNull(metastoreUrl, String.format("%s should be set", METASTORE_URL));
      actualBuilder = InternalCatalogBuilder.builder().metastoreUrl(metastoreUrl);

      actualCatalogName = options.get(ArcticValidator.ARCTIC_CATALOG.key());
      Preconditions.checkNotNull(actualCatalogName, String.format("%s should be set",
          ArcticValidator.ARCTIC_CATALOG.key()));
    }

    if (options.containsKey(ArcticValidator.ARCTIC_DATABASE.key()) &&
        options.containsKey(ArcticValidator.ARCTIC_TABLE.key())) {
      objectPath = new ObjectPath(options.get(ArcticValidator.ARCTIC_DATABASE.key()),
          options.get(ArcticValidator.ARCTIC_TABLE.key()));
    } else {
      objectPath = new ObjectPath(identifier.getDatabaseName(), identifier.getObjectName());
    }

    ArcticTableLoader tableLoader = createTableLoader(objectPath, actualCatalogName, actualBuilder, options);

    ArcticTable arcticTable = ArcticUtils.loadArcticTable(tableLoader);
    ScanTableSource arcticDynamicSource;

    String readMode = PropertyUtil.propertyAsString(arcticTable.properties(),
        ArcticValidator.ARCTIC_READ_MODE, ArcticValidator.ARCTIC_READ_MODE_DEFAULT);

    boolean dimTable = PropertyUtil.propertyAsBoolean(arcticTable.properties(),
        ArcticValidator.DIM_TABLE_ENABLE.key(), ArcticValidator.DIM_TABLE_ENABLE.defaultValue());
    TableSchema tableSchema = com.netease.arctic.flink.FlinkSchemaUtil.getPhysicalSchema(catalogTable.getSchema(),
        dimTable);
    switch (readMode) {
      case ArcticValidator.ARCTIC_READ_FILE:
        LOG.info("build file reader");
        arcticDynamicSource = new ArcticFileSource(tableLoader, tableSchema, arcticTable, context.getConfiguration());
        break;
      case ArcticValidator.ARCTIC_READ_LOG:
      default:
        arcticDynamicSource = createLogSource(arcticTable, context);
    }

    return new ArcticDynamicSource(identifier.getObjectName(), arcticDynamicSource, arcticTable, tableSchema,
        arcticTable.properties());
  }

  @Override
  public ArcticDynamicSink createDynamicTableSink(Context context) {
    CatalogTable catalogTable = context.getCatalogTable();

    ObjectIdentifier identifier = context.getObjectIdentifier();
    Map<String, String> options = catalogTable.getOptions();

    final String topic = options.get(TableProperties.LOG_STORE_MESSAGE_TOPIC);

    ArcticTableLoader tableLoader = createTableLoader(
        new ObjectPath(identifier.getDatabaseName(), identifier.getObjectName()),
        internalCatalogName, internalCatalogBuilder, options);

    ArcticTable table = ArcticUtils.loadArcticTable(tableLoader);
    return new ArcticDynamicSink(
        catalogTable,
        tableLoader,
        topic,
        table.isKeyedTable()
    );
  }

  private static ArcticTableLoader createTableLoader(ObjectPath tablePath,
                                                     String internalCatalogName,
                                                     InternalCatalogBuilder catalogBuilder,
                                                     Map<String, String> flinkTableProperties) {
    TableIdentifier identifier = TableIdentifier.of(
        internalCatalogName,
        tablePath.getDatabaseName(),
        tablePath.getObjectName());

    return ArcticTableLoader.of(identifier, catalogBuilder, flinkTableProperties);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(TOPIC);
    options.add(PROPS_BOOTSTRAP_SERVERS);
    options.add(PROPS_GROUP_ID);
    options.add(SCAN_STARTUP_MODE);
    options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
    options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
    options.add(SINK_PARTITIONER);
    options.add(ArcticValidator.ARCTIC_CATALOG);
    options.add(ArcticValidator.ARCTIC_TABLE);
    options.add(ArcticValidator.ARCTIC_DATABASE);
    options.add(ArcticValidator.DIM_TABLE_ENABLE);
    options.add(METASTORE_URL_OPTION);
    return options;
  }

  private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
      FactoryUtil.TableFactoryHelper helper) {
    final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
        helper.discoverOptionalDecodingFormat(
            DeserializationFormatFactory.class, KEY_FORMAT);
    keyDecodingFormat.ifPresent(
        format -> {
          if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                String.format(
                    "A key format should only deal with INSERT-only records. " +
                        "But %s has a changelog mode of %s.",
                    helper.getOptions().get(KEY_FORMAT),
                    format.getChangelogMode()));
          }
        });
    return keyDecodingFormat;
  }

  private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
      FactoryUtil.TableFactoryHelper helper) {
    return helper.discoverOptionalDecodingFormat(
            DeserializationFormatFactory.class, FactoryUtil.FORMAT)
        .orElseGet(
            () ->
                helper.discoverDecodingFormat(
                    DeserializationFormatFactory.class, VALUE_FORMAT));
  }

  private LogDynamicSource createLogSource(ArcticTable arcticTable, Context context) {
    CatalogTable catalogTable = context.getCatalogTable();
    TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(catalogTable.getSchema());
    Schema schema = FlinkSchemaUtil.convert(physicalSchema);
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

    ReadableConfig tableOptions = helper.getOptions();

    final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
        getKeyDecodingFormat(helper);
    final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
        getValueDecodingFormat(helper);
    validateTableSourceOptions(tableOptions);
    final KafkaOptions.StartupOptions startupOptions = getStartupOptions(tableOptions);
    final Properties properties = KafkaOptions.getKafkaProperties(catalogTable.getOptions());

    // add topic-partition discovery
    properties.setProperty(
        FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
        String.valueOf(
            tableOptions
                .getOptional(SCAN_TOPIC_PARTITION_DISCOVERY)
                .map(Duration::toMillis)
                .orElse(FlinkKafkaConsumerBase.PARTITION_DISCOVERY_DISABLED)));

    final DataType physicalDataType =
        catalogTable.getSchema().toPhysicalRowDataType();

    final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

    final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

    final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

    LOG.info("build log source");
    return new LogDynamicSource(
        physicalDataType,
        keyDecodingFormat.orElse(null),
        valueDecodingFormat,
        keyProjection,
        valueProjection,
        keyPrefix,
        KafkaOptions.getSourceTopics(tableOptions),
        KafkaOptions.getSourceTopicPattern(tableOptions),
        properties,
        startupOptions.startupMode,
        startupOptions.specificOffsets,
        startupOptions.startupTimestampMillis,
        false,
        arcticTable.isKeyedTable() &&
            arcticTable.asKeyedTable().primaryKeySpec().primaryKeyExisted(),
        schema,
        tableOptions,
        arcticTable.name());
  }

}
