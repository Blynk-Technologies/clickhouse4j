[![Maven Central](https://maven-badges.herokuapp.com/maven-central/cc.blynk.clickhouse/clickhouse4j/badge.svg)](https://github.com/blynkkk/clickhouse4j) [![clickhouse4j](https://travis-ci.org/blynkkk/clickhouse4j.svg?branch=master)](https://github.com/blynkkk/clickhouse4j)

Clickhouse4j - lighter and faster alternative for the official ClickHouse JDBC driver
===============

The main differences between this and official driver are:

- Smaller size. 850kb vs 5.6mb of the original driver (**7x smaller jar size**);
- All unnecessary dependencies (Apache HttpClient, Jackson, Guava) were removed;
- A bunch of micro optimizations were applied (for example, **batch inserts are now 40% faster**);
- Checkstyle applied to the code to make contributions easier;
- Allow to set unknown number of values for IN clause
- Only 2 dependencies left: lz4 and slf4j and updated to the latest version;
- Compiled against Java 8;
- Cleaned up all code and IDE warnings;
- Decreased visibility of public methods;
- Simplified code structure, removed unnecessary inheritance, removed many useless legacy code;


### Usage
```xml
<dependency>
    <groupId>cc.blynk.clickhouse</groupId>
    <artifactId>clickhouse4j</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Migration from the offical driver

All you need to do is replace:

`ru.yandex.clickhouse.ClickHouseDriver` to `cc.blynk.clickhouse.ClickHouseDriver`

URL syntax: 
`jdbc:clickhouse://<host>:<port>[/<database>]`, e.g. `jdbc:clickhouse://localhost:8123/test`

JDBC Driver Class:
`cc.blynk.clickhouse.ClickHouseDriver`

additionally, if you have a few instances, you can use `BalancedClickhouseDataSource`.

### Build requirements

In order to build the jdbc client one need to have jdk 1.8 or higher.

### Compiling with maven

`mvn package -DskipTests=true`

To build a jar with dependencies use

`mvn package assembly:single -DskipTests=true`
