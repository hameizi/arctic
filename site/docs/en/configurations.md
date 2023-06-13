## Multi-level Configuration Management

Arctic provides configurations that can be configured at the `Catalog`, `Table`, and `Engine` levels. The configuration priority is given first to the `Engine`, followed by the `Table`, and finally by the `Catalog`. Generally, we recommend users to set default values in the `Catalog`, such as Self-optimizing related configurations. We also recommend users to specify customized configurations when [`Create Table`]((guides/managing-tables.md##_1)), which can also be modified through [`Alter Table`](guides/managing-tables.md##_2) operations. If tuning is required in the `Engine`, then consider configuring it at the Engine level.

## Self-optimizing Configurations

Self-optimizing configurations are applicable to both Iceberg Format and Mixed streaming Format.

| Key                                                 | Default          | Description                                              |
|-----------------------------------------------------|------------------|---------------------------------------------------|
| self-optimizing.enabled                             | true             | Enables Self-optimizing                                |
| self-optimizing.group                               | default          | Optimizer group for Self-optimizing                                   |
| self-optimizing.quota                               | 0.1              | Quota for Self-optimizing, indicating the CPU resource the table can take up                       |
| self-optimizing.num-retries                         | 5                | Number of retries after failure of Self-optimizing                       |
| self-optimizing.target-size                         | 134217728（128MB）| Target size for Self-optimizing                           |
| self-optimizing.max-file-count                      | 10000            | Maximum number of files processed by a Self-optimizing process              |               |
| self-optimizing.fragment-ratio                      | 8                | The fragment file size threshold. We could divide self-optimizing.target-size by this ratio to get the actual fragment file size           |
| self-optimizing.minor.trigger.file-count            | 12               | The minimum numbers of fragment files to trigger minor optimizing   |
| self-optimizing.minor.trigger.interval              | 3600000（1 hour）  | The time interval in milliseconds to trigger minor optimizing                         |
| self-optimizing.major.trigger.duplicate-ratio       | 0.5              | The ratio of duplicate data of segment files to trigger major optimizing  |
| self-optimizing.full.trigger.interval               | -1（closed）         | The time interval in milliseconds to trigger full optimizing

## Data-cleaning Configurations

Data-cleaning configurations are applicable to both Iceberg Format and Mixed streaming Format.

| Key                                                 | Default          | Description                                              |
|---------------------------------------------|-----------|------------------------------------|
| table-expire.enabled                        | true      | Enables periodically expire table                      |
| change.data.ttl.minutes                     | 10080（7 days） | Time to live in minutes for data of ChangeStore                |
| snapshot.change.keep.minutes                | 10080（7 days） | Table-Expiration keeps the latest snapshots of ChangeStore within a specified time in minutes            |
| snapshot.base.keep.minutes                  | 720（12 hours） | Table-Expiration keeps the latest snapshots of BaseStore within a specified time in minutes                |
| clean-orphan-file.enabled                   | false     | Enables periodically clean orphan files                       |
| clean-orphan-file.min-existing-time-minutes | 2880（2 days）  | Cleaning orphan files keeps the files modified within a specified time in minutes |

## Mixed Format Configurations

If using Iceberg Format，please refer to [Iceberg configurations](https://iceberg.apache.org/docs/latest/configuration/)，the following configurations are only applicable to Mixed Format.

### Reading Configurations

| Key                                                 | Default          | Description                                              |
| ---------------------------------- | ---------------- | ----------------------------------       |
| read.split.open-file-cost          | 4194304（4MB）    | The estimated cost to open a file                        |
| read.split.planning-lookback       | 10               | Number of bins to consider when combining input splits               |
| read.split.target-size              | 134217728（128MB）| Target size when combining data input splits                     |

### Writing Configurations

| Key                                                 | Default          | Description                                              |
| ---------------------------------- | ---------------- | ----------------------------------       |
| base.write.format                  | parquet          | File format for the table for BaseStore, applicable to KeyedTable       |
| change.write.format                | parquet          | File format for the table for ChangeStore, applicable to KeyedTable    |
| write.format.default               | parquet          | Default file format for the table, applicable to UnkeyedTable          |
| base.file-index.hash-bucket        | 4                | Initial number of buckets for BaseStore auto-bucket         |
| change.file-index.hash-bucket      | 4                | Initial number of buckets for ChangeStore auto-bucket       |
| write.target-file-size-bytes       | 134217728（128MB）| Target size when writing                     |
| write.upsert.enabled               | false            | Enable upsert mode, multiple insert data with the same primary key will be merged if enabled   |
| write.distribution-mode            | hash             | Shuffle rules for writing. UnkeyedTable can choose between none and hash, while KeyedTable can only choose hash           |
| write.distribution.hash-mode       | auto             | Auto-bucket mode, which supports primary-key, partition-key, primary-partition-key, and auto  |

### LogStore Configurations

| Key                                | Default          | Description                                              |
| ---------------------------------- | ---------------- | ----------------------------------       |
| log-store.enabled                  | false            | Enables LogStore                        |
| log-store.type                     | kafka            | Type of LogStore, which supports 'kafka' and 'pulsar'            |
| log-store.address                  | NULL             | Address of LogStore, required when LogStore enabled. For Kafka, this is the Kafka bootstrap servers. For Pulsar, this is the Pulsar Service URL, such as 'pulsar://localhost:6650'|
| log-store.topic                    | NULL             | Topic of LogStore, required when LogStore enabled
| log-store.consistency-guarantee.enabled   | false     | Enables consistency guarantees. This is only supported when log-store.type=kafka|
| properties.pulsar.admin.adminUrl   | NULL             | HTTP URL of Pulsar admin, such as 'http://my-broker.example.com:8080'. Only required when log-store.type=pulsar|
| properties.XXX                     | NULL             | Other configurations of LogStore. <br><br>For Kafka, all the configurations supported by Kafka Consumer/Producer can be set by prefixing them with `properties.`，<br>such as `'properties.batch.size'='16384'`，<br>refer to [Kafka Consumer Configurations](https://kafka.apache.org/documentation/#consumerconfigs), [Kafka Producer Configurations](https://kafka.apache.org/documentation/#producerconfigs) for more details.<br><br> For Pulsar，all the configurations supported by Pulsar can be set by prefixing them with `properties.`, <br>such as `'properties.pulsar.client.requestTimeoutMs'='60000'`，<br>refer to [Flink-Pulsar-Connector](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/datastream/pulsar) for more details


### Watermark Configurations

| Key                                           | Default          | Description                                              |
| ----------------------------------------------| ------------------- | ----------------------------------       |
| table.event-time-field                        | _ingest_time        | The event time field for calculating the watermark. The default `_ingest_time` indicates calculating with the time when the data was written |
| table.watermark-allowed-lateness-second       | 0                   | The allowed lateness time in seconds when calculating watermark          |
| table.event-time-field.datetime-string-format | `yyyy-MM-dd HH:mm:ss` | The format of event time when it is in string format           |
| table.event-time-field.datetime-number-format | TIMESTAMP_MS | The format of event time when it is in numeric format, which supports TIMESTAMP_MS (timestamp in milliseconds) and TIMESTAMP_S (timestamp in seconds)|

### Mixed Hive Format Configurations

| Key                                | Default          | Description                                              |
| ---------------------------------- | ---------------- | ----------------------------------       |
| base.hive.auto-sync-schema-change  | true             | Whether synchronize schema changes of Hive Table from HMS             |
| base.hive.auto-sync-data-write     | false            | Whether synchronize data changes of Hive Table from HMS, this should be true when writing to Hive    |