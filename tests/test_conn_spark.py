from conn.spark import create_spark_context

spark = create_spark_context('test')

spark.sql("SELECT * FROM b284_cld_iceberg.iceberg_schema.dm_krd_kredirisk limit 5").show()