Users can download the stable 0.4.0 release zip package from Github or download the source code and compile it according to the README.

## System Requirements

- Java 8 is required. Java 17 is required for Trino.
- Optional: MySQL 5.5 or higher, or MySQL 8
- Optional: ZooKeeper 3.4.x or higher
- Optional: Hive (2.x or 3.x)
- Optional: Hadoop (2.9.x or 3.x)

## Download the distribution

You can find the released versions at [https://github.com/NetEase/arctic/releases](https://github.com/NetEase/arctic/releases), in addition to downloading
You can download arctic-x.y.z-bin.zip (x.y.z is the release number), but you can also download the runtime packages for each engine version according to the engine you are using. Execute unzip
arctic-x.y.z-bin.zip and unzip it to create the arctic-x.y.z directory in the same directory, and then go to the arctic-x.y.z directory.

## Source code compilation

You can build based on the master branch without compiling Trino. The compilation method and the directory of results are described below

```shell
$ git clone https://github.com/NetEase/arctic.git
$ cd arctic
$ mvn clean package -DskipTests -pl '!Trino' [-Dcheckstyle.skip=true]
$ cd dist/target/
$ ls
arctic-x.y.z-bin.zip (Target Pakcage)
dist-x.y.z-tests.jar
dist-x.y.z.jar
archive-tmp/
maven-archiver/

$ cd ../../flink/v1.12/flink-runtime/target
$ ls 
arctic-flink-runtime-1.12-x.y.z-tests.jar
arctic-flink-runtime-1.12-x.y.z.jar (Flink 1.12 Target flink runtime package)
original-arctic-flink-runtime-1.12-x.y.z.jar
maven-archiver/

Or switch from the dist/target directory to the spark runtime package directory
$ spark/v3.1/spark-runtime/target
$ ls
arctic-spark-3.1-runtime-0.4.0.jar (spark v3.1 Target flink runtime package)
arctic-spark-3.1-runtime-0.4.0-tests.jar
arctic-spark-3.1-runtime-0.4.0-sources.jar
original-arctic-spark-3.1-runtime-0.4.0.jar
```

If you need to compile the Trino module at the same time, you need to install jdk17 locally and configure toolchains.xml in the user's ${user.home}/.m2/ directory, then run mvn
package -P toolchain to compile the entire project.

```shell
<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
    <toolchain>
        <type>jdk</type>
        <provides>
            <version>17</version>
            <vendor>sun</vendor>
        </provides>
        <configuration>
            <jdkHome>${YourJDK17Home}</jdkHome>
        </configuration>
    </toolchain>
</toolchains>
```

## Configuring AMS

If you want to use AMS in a formal scenario, it is recommended to modify `{ARCTIC_HOME}/conf/config.yaml` by referring to the following configuration steps.

### Configure the service address

- The arctic.ams.server-host.prefix configuration selects the IP address or segment prefix for your service bindings, with the intent that in HA
  mode, users can use the same configuration file on multiple hosts; if you deploy only a single node, this configuration can also directly specify the full IP address.
- AMS itself provides http service and thrift service, you need to configure the ports that these two services listen on. 1630 is the default port for Http service and 1260 is the default port for Thrift service.

```shell
ams:
  arctic.ams.server-host.prefix: "127." #To facilitate batch deployment can config server host prefix.Must be enclosed in double quotes
  arctic.ams.thrift.port: 1260   # ams thrift service port
  arctic.ams.http.port: 1630    # ams dashboard service port
```

???+ Attention

    make sure the port is not used before configuring it

### Configuration System Database

Users can use MySQL as the system database instead of Derby. To do so, the system database must first be initialized in MySQL：

```shell
$ mysql -h{mysql-host-IP} -P{mysql-port} -u{username} -p
Enter password: 
'Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 41592724
Server version: 5.7.20-v3-log Source distribution

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
mysql> create database arctic;
Query OK, 1 row affected (0.01 sec)

mysql> use arctic;
Database changed
mysql> source {ARCTIC_HOME}/conf/mysql/x.y.z-init.sql
```

Add MySQL configuration under `ams`:

```shell
ams:
  arctic.ams.mybatis.ConnectionURL: jdbc:mysql://{host}:{port}/{database}?useUnicode=true&characterEncoding=UTF8&autoReconnect=true&useAffectedRows=true&useSSL=false
  arctic.ams.mybatis.ConnectionDriverClassName: com.mysql.jdbc.Driver
  arctic.ams.mybatis.ConnectionUserName: {user}
  arctic.ams.mybatis.ConnectionPassword: {password}
  arctic.ams.database.type: mysql
```

### Configuring high availability

To improve stability, AMS supports a one-master-multi-backup HA mode. Zookeeper is used to implement leader election,
and the AMS cluster name and Zookeeper address are specified. The AMS cluster name is used to bind different AMS clusters
on the same Zookeeper cluster to avoid mutual interference.

```shell
ams:
  #HA config
  arctic.ams.ha.enable: true     #enable ha
  arctic.ams.cluster.name: default  # Different AMS clusters are bound on the same Zookeeper cluster
  arctic.ams.zookeeper.server: 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183  # zookeeper server地址
```

### 配置 Optimizer

Self-optimizing 需要配置 optimizer 资源，包含 Containers 配置和 Optimizer group 配置。以配置 Flink 类型的 Optimizer
为例，配置如下， 详细的参数说明及其它类型的配置见 [managing-optimizers](managing-optimizers.md)

```shell
containers:
  - name: flinkContainer
    type: flink
    properties:
      FLINK_HOME: /opt/flink/        #flink install home
      HADOOP_CONF_DIR: /etc/hadoop/conf/       #hadoop config dir
      HADOOP_USER_NAME: hadoop       #hadoop user submit on yarn
      JVM_ARGS: -Djava.security.krb5.conf=/opt/krb5.conf       #flink launch jvm args, like kerberos config when ues kerberos
      FLINK_CONF_DIR: /etc/hadoop/conf/        #flink config dir
optimize_group:
  - name: flinkOp
    # container name, should be in the names of containers  
    container: flinkContainer
    properties:
      taskmanager.memory: 2048
      jobmanager.memory: 1024
```

一个完整的配置样例如下：

```shell
ams:
  arctic.ams.server-host.prefix: "127." #To facilitate batch deployment can config server host prefix.Must be enclosed in double quotes
  arctic.ams.thrift.port: 1260   # ams thrift服务访问的端口
  arctic.ams.http.port: 1630    # ams dashboard 访问的端口
  arctic.ams.optimize.check.thread.pool-size: 10
  arctic.ams.optimize.commit.thread.pool-size: 10
  arctic.ams.expire.thread.pool-size: 10
  arctic.ams.orphan.clean.thread.pool-size: 10
  arctic.ams.file.sync.thread.pool-size: 10
  # derby config.sh 
  # arctic.ams.mybatis.ConnectionDriverClassName: org.apache.derby.jdbc.EmbeddedDriver
  # arctic.ams.mybatis.ConnectionURL: jdbc:derby:/tmp/arctic/derby;create=true
  # arctic.ams.database.type: derby
  # mysql config
  arctic.ams.mybatis.ConnectionURL: jdbc:mysql://{host}:{port}/{database}?useUnicode=true&characterEncoding=UTF8&autoReconnect=true&useAffectedRows=true&useSSL=false
  arctic.ams.mybatis.ConnectionDriverClassName: com.mysql.jdbc.Driver
  arctic.ams.mybatis.ConnectionUserName: {user}
  arctic.ams.mybatis.ConnectionPassword: {password}
  arctic.ams.database.type: mysql

  #HA config
  arctic.ams.ha.enable: true     #开启ha
  arctic.ams.cluster.name: default  # 区分同一套zookeeper上绑定多套AMS
  arctic.ams.zookeeper.server: 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183

  # Kyuubi config
  arctic.ams.terminal.backend: kyuubi
  arctic.ams.terminal.kyuubi.jdbc.url: jdbc:hive2://127.0.0.1:10009/
  
  # login config
  login.username: admin
  login.password: admin

# extension properties for like system
extension_properties:
#test.properties: test
containers:
  # arctic optimizer container config.sh
  - name: localContainer
    type: local
    properties:
      hadoop_home: /opt/hadoop
      # java_home: /opt/java
  - name: flinkContainer
    type: flink
    properties:
      FLINK_HOME: /opt/flink/        #flink install home
      HADOOP_CONF_DIR: /etc/hadoop/conf/       #hadoop config dir
      HADOOP_USER_NAME: hadoop       #hadoop user submit on yarn
      JVM_ARGS: -Djava.security.krb5.conf=/opt/krb5.conf       #flink launch jvm args, like kerberos config when ues kerberos
      FLINK_CONF_DIR: /etc/hadoop/conf/        #flink config dir
  - name: externalContainer
    type: external
    properties:
optimize_group:
  - name: default
    # container name, should equal with the name that containers config.sh
    container: localContainer
    properties:
      # unit MB
      memory: 1024
  - name: flinkOp
    container: flinkContainer
    properties:
      taskmanager.memory: 1024
      jobmanager.memory: 1024
  - name: externalOp
    container: external
    properties:
```

### 配置 Terminal

Terminal 在 local 模式执行的情况下，可以配置 Spark 相关参数

```shell
arctic.ams.terminal.backend: local
arctic.ams.terminal.local.spark.sql.session.timeZone: UTC
arctic.ams.terminal.local.spark.sql.iceberg.handle-timestamp-without-timezone: false
# When the catalog type is hive, using spark session catalog automatically in the terminal to access hive tables
arctic.ams.terminal.local.using-session-catalog-for-hive: true
```

## 启动 AMS

进入到目录 arctic-x.y.z ， 执行 bin/ams.sh start 启动 AMS。

```shell
$ cd arctic-x.y.z
$ bin/ams.sh start
```

然后通过浏览器访问 http://localhost:1630 可以看到登录界面,则代表启动成功，登录的默认用户名和密码都是 admin。
