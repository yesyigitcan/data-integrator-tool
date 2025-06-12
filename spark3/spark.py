from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from conf import read_spark_properties, read_catalog_properties
from typing import Union

def create_spark_context(appname: Union[str, None] = None) -> SparkContext:
    conf_spark: dict = read_spark_properties()['configs']
    conf_catalog: dict = read_catalog_properties()['configs']

    conf: dict = {**conf_spark, **conf_catalog}
    
    spark = SparkSession.builder.appName(appname) if appname else SparkSession.builder
    for key, value in conf.items():
        spark = spark.config(key, value)
    return spark.getOrCreate()

def execute_dataframe_operation(df: DataFrame, ops: dict):
    for op, args in ops.items():
        if op == 'drop':
            df = df.drop(args)
        elif op == 'select':
            df = df.select(*[arg.strip() for arg in args.split(',')])
        elif op == 'withColumn':
            for colName, exprStr in args.items():
                df = df.withColumn(colName, f.expr(exprStr))
        elif op == 'show':
            df.show(args)
    return df

def cast_dataframe(df: DataFrame, schema: dict):
    for colname, typ in schema.items():
        df = df.withColumn(colname, f.col(colname).cast(typ))
    return df
