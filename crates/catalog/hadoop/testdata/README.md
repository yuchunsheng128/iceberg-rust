# Hadoop Test Data

`./hadoop_warehouse` contains 2 namespaces:

* `test`: Setup with 2 tables, `my_table_1` and `my_table_2`. Each table contains 2 rows:
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
* `nested.test`: Setup with 1 table, `my_table_3`. The table contains 2 rows:
    * `my_table_2`:
        ```console
+---+----+
| id|name|
+---+----+
|  5| foo|
|  6| bar|
+---+----+
        ```

`./hadoop_warehouse` was generated with `spark-shell`:
```bash
./spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.2

spark.conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
spark.conf.set("spark.sql.catalog.hadoop_prod.warehouse", "file:///tmp/hadoop_warehouse")

spark.sql("CREATE NAMESPACE hadoop_prod.test")
spark.sql("CREATE TABLE hadoop_prod.test.my_table_1 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.test.my_table_1 VALUES (1, 'foo'), (2, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.test.my_table_1").show()
spark.sql("CREATE TABLE hadoop_prod.test.my_table_2 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.test.my_table_2 VALUES (3, 'foo'), (4, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.test.my_table_2").show()

spark.sql("CREATE NAMESPACE hadoop_prod.nested.test")
spark.sql("CREATE TABLE hadoop_prod.nested.test.my_table_3 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.nested.test.my_table_3 VALUES (5, 'foo'), (6, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.nested.test.my_table_3").show()
```