# Trino

## Iceberg format
Iceberg format refers to the original Iceberg table, which can be accessed using the Connector provided by Trino for Iceberg natively.
lease refer to the documentation at [Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html#) for more information. 

## Mixed format
### Install

- Create the {trino_home}/plugin/arctic directory in the Trino installation package, 
  and extract the contents of the arctic-trino package trino-arctic-xx-SNAPSHOT.tar.gz to the {trino_home}/plugin/arctic directory.
- Configure the Catalog configuration file for Arctic in the {trino_home}/etc/catalog directory, for example:

```tex
connector.name=arctic
arctic.url=thrift://{ip}:{port}/{catalogName}
```

### Support SQL statement

#### Query Table

By adopting the Merge-On-Read approach to read Mixed Format, the latest data of the table can be read, for example:

```sql
SELECT * FROM "{TABLE_NAME}"
```



#### Query BaseStore

Directly querying the BaseStore in a table with a primary key is supported. The BaseStore stores the stock data of the table, which is usually generated by batch job or optimization. 
The queried data is static and the query efficiency is very high, but the timeliness is not good. The syntax is as follows:

```sql
SELECT * FROM "{TABLE_NAME}#BASE"
```



#### Query ChangeStore

Directly querying the ChangeStore in a table with a primary key is supported. The ChangeStore stores the stream and change data of the table, which is usually written in real time by stream job.
The change records of the table can be queried through the ChangeStore, and the expiry time of the data in the ChangeStore determines how long ago the change records can be queried.

```sql
SELECT * FROM "{TABLE_NAME}#CHANGE"
```

Three additional columns will be included in the query result, which are:

- _transaction_id: The transaction ID allocated by AMS when the data is written. In batch mode, it is allocated for each SQL execution, and in stream mode, it is allocated for each checkpoint.
- _file_offset：Indicates the order in which the data was written in the same batch of _transaction_id.
- _change_action：Indicates the type of data, which can be either INSERT or DELETE.

#### Trino and Arctic Type Mapping:

| Arctic type   | Trino type                    |
| :------------- | :---------------------------- |
| `BOOLEAN`      | `BOOLEAN`                     |
| `INT`          | `INTEGER`                     |
| `LONG`         | `BIGINT`                      |
| `FLOAT`        | `REAL`                        |
| `DOUBLE`       | `DOUBLE`                      |
| `DECIMAL(p,s)` | `DECIMAL(p,s)`                |
| `DATE`         | `DATE`                        |
| `TIME`         | `TIME(6)`                     |
| `TIMESTAMP`    | `TIMESTAMP(6)`                |
| `TIMESTAMPTZ`  | `TIMESTAMP(6) WITH TIME ZONE` |
| `STRING`       | `VARCHAR`                     |
| `UUID`         | `UUID`                        |
| `BINARY`       | `VARBINARY`                   |
| `STRUCT(...)`  | `ROW(...)`                    |
| `LIST(e)`      | `ARRAY(e)`                    |
| `MAP(k,v)`     | `MAP(k,v)`                    |

### Trino uses proxy user to access Hadoop cluster.
By default, when Trino queries Arctic, it uses the Hadoop user configured in the [catalog creation](../../ch/guides/managing-catalogs.md#catalog) to access the Hadoop cluster.
To use Trino's user to access the Hadoop cluster, you need enable Hadoop impersonation by adding the arctic.hdfs.impersonation.enabled=true parameter in the Arctic catalog configuration file located in the {trino_home}/etc/catalog directory, as follows.

```tex
connector.name=arctic
arctic.url=thrift://{ip}:{port}/{catalogName}
arctic.hdfs.impersonation.enabled=true
```
`arctic.hdfs.impersonation.enabled` default false

???+ note

    To use Hadoop impersonation, you need to enable the proxy feature for the Hadoop user configured in the [catalog creation](../guides/managing-catalogs.md#catalog) in the Hadoop cluster beforehand，
    and make sure that it can proxy the Trino querying user. Please refer to [Hadoop Proxy User](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html#Configurations) for more information. 