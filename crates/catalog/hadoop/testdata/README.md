# Hadoop Test Data

* `./hadoop_warehouse` contains a single namespace `test` with a 2 tables: `my_table_1` and `my_table_2`. Each table contains 2 rows.
    * `my_table_1`:
        ```console
+---+----+
| id|name|
+---+----+
|  1| foo|
|  2| bar|
+---+----+
        ```
    * `my_table_2`:
        ```console
+---+----+
| id|name|
+---+----+
|  3| foo|
|  4| bar|
+---+----+
        ```
* `./hadoop_warehouse` was generated with `spark-shell`:
    ```bash
./spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.2

spark.conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
spark.conf.set("spark.sql.catalog.hadoop_prod.warehouse", "file:///tmp/multi_table_warehouse")

spark.sql("CREATE NAMESPACE hadoop_prod.test")
spark.sql("CREATE TABLE hadoop_prod.test.my_table_1 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.test.my_table_1 VALUES (1, 'foo'), (2, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.test.my_table_1").show()
spark.sql("CREATE TABLE hadoop_prod.test.my_table_2 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.test.my_table_2 VALUES (3, 'foo'), (4, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.test.my_table_2").show()
    ```