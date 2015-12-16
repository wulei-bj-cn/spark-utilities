# spark-utilities
Utilities/applications that are built on top of Apache Spark, developed to ease end users' use cases.

Currently this project contains two practical utilities:<br>
1. sql.query<br>
2. common.toParquet<br>

##1. sql.query

This utility is similar to Spark's default spark-sql CLI, only that it provides more convenience and possibilities. Furthermore, it's tested and verified that query performance by invoking this utility is better than query performance by invoking default spark-sql CLI in most use cases.

Advantages of this sql.query utility over spark-sql CLI:

####`1) Requires no dependence on Hive`<br>
You don't have to install Hive in the same cluster where Spark is deployed, no need to move hive-site.xml to Spark's conf diretory to make Spark recognize Hive registered tables. Further, you don't have to recompile Spark to support Hive, thus to support SQL queries by invoking spark-sql CLI.

####`2) More choices for displaying query results`<br>
In spark-sql CLI, you can only print your query results to your current session terminal, when the query result is large, you may lose some result sets and can only obtain a partial query result. With sql.query utility, however, you can either print your query results to current session terminal or store your results directly onto HDFS.

##2. common.toParquet

This utility is to convert a text file to a parquet file given a table-schema.xml configuration file.
