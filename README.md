Clickhouse4j - lighter and faster alternative for the official ClickHouse JDBC driver
===============

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/cc.blynk.clickhouse/clickhouse4j/badge.svg)](https://mvnrepository.com/artifact/cc.blynk.clickhouse/clickhouse4j) [![clickhouse4j](https://travis-ci.org/blynkkk/clickhouse4j.svg?branch=master)](https://github.com/blynkkk/clickhouse4j)

The main differences between this and the official driver are:

- Smaller size. 850kb vs 5.6mb of the original driver (**7x smaller jar size**)
- A bunch of micro optimizations were applied (for example, **batch inserts are now 40% faster**)
- ```CopyManager``` added;
- Compiled against Java 8 and many [other things](https://github.com/blynkkk/clickhouse4j/blob/master/CHANGELOG)

### Usage
```xml
<dependency>
    <groupId>cc.blynk.clickhouse</groupId>
    <artifactId>clickhouse4j</artifactId>
    <version>1.1.1</version>
</dependency>
```

### CopyManager usage
```CopyManager``` is utility class that helps to read the queries from / to the file.

#### Writing the sql result from the DB directly to the file

```
String query = "SELECT * from copy_manager_test.csv_data FORMAT CSVWithNames";
Path outputFile = ...;

try (CopyManager copyManager = CopyManagerFactory.create(dataSource)) {
    copyManager.copyFromDb(query, outputFile);
}
//outputFile now has all the data and headers from the copy_manager_test DB and csv_data table
```

#### Reading from the file and forwarding it to the DB

```
String query = "INSERT INTO copy_manager_test.csv_data FORMAT CSV";
Path inputFile = ...;

try (CopyManager copyManager = CopyManagerFactory.create(dataSource)) {
    copyManager.copyToDb(query, inputFile);
}

//DB copy_manager_test and csv_data table now has all csv data from the inputFile
```

### Migration from the official driver

All you need to do is replace:

`ru.yandex.clickhouse.ClickHouseDriver` to `cc.blynk.clickhouse.ClickHouseDriver`

URL syntax: 
`jdbc:clickhouse://<host>:<port>[/<database>]`, e.g. `jdbc:clickhouse://localhost:8123/test`

JDBC Driver Class:
`cc.blynk.clickhouse.ClickHouseDriver`

additionally, if you have a few instances, you can use `BalancedClickhouseDataSource`.

### Build requirements

In order to build the jdbc client one needs to have jdk 1.8 or higher.

### Compiling with maven

`mvn package -DskipTests=true`

To build a jar with dependencies use

`mvn package assembly:single -DskipTests=true`
