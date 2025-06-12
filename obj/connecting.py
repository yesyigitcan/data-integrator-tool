from conf import read_jdbc_properties, read_catalog_properties
from spark3.spark import create_spark_context
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod
from cryptography.fernet import Fernet
from typing import Union
import os
class Connection(ABC):
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
    @abstractmethod
    def read(self, sql:str) -> DataFrame:
        pass
    @abstractmethod
    def sql(self, sql:str) -> DataFrame:
        pass
    @abstractmethod
    def write(self, df:DataFrame, tablename: str):
        pass

class JDBCConnection(Connection):
    def __init__(self, conn_id):
        super().__init__(conn_id)
    def read(self, sql:str, options: Union[dict, None]) -> DataFrame:
        properties = read_jdbc_properties()[self.conn_id]['options']
        options = options if options else dict()
        
        # Remove query-related properties
        if 'dbtable' in properties:
            properties.pop('dbtable')

        f = Fernet(os.environ['DIT_TOKEN'])
        properties['password'] = f.decrypt(properties['password'].encode()).decode()

        spark = create_spark_context()
        jdbc = spark\
            .read\
            .format('jdbc')
        
        for key, value in {**properties, **options}.items():
            jdbc = jdbc.option(key, value)

        return jdbc.option("dbtable", 
                           f"({sql}) JDBC_CONNECTION_QUERY").load()
    def sql(self, sql):
        return super().sql(sql)
    def write(self, df:DataFrame, tablename: str):
        properties = read_jdbc_properties()

        # Remove query-related properties
        if 'dbtable' in properties:
            properties.pop('dbtable')

        f = Fernet(os.environ['DIT_TOKEN'])
        properties['password'] = f.decrypt(properties['password'].encode()).decode()

        jdbc = df.write.format('jdbc')
        for key, value in properties.items():
            jdbc = jdbc.option(key, value)

        jdbc.option("dbtable", tablename).mode('append').save()
    
class CatalogConnection(Connection):
    def __init__(self):
        super().__init__(None)
    def read(self, sql, options: Union[dict, None]) -> DataFrame:
        options = options if options else dict()
        return create_spark_context().sql(sql)
    def sql(self, sql):
        return create_spark_context().sql(sql)
    def write(self, df: DataFrame, tablename: str):
        df.write.format('iceberg').mode('append').save(tablename)
        

class FileConnection(Connection):
    def __init__(self):
        super().__init__(None)
    def read(self, path, options: Union[dict, None]) -> DataFrame:
        options = options if options else dict()
        return create_spark_context().read.option("multiline","true").json(path).cache()
    def sql(self, sql):
        raise Exception('Unknown command sql for FileConnection')
    def write(self, df, path):
        df.write.json(path)