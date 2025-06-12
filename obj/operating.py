from collections import deque, defaultdict
from typing import Union
from obj.connecting import Connection
import uuid
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from spark3.spark import execute_dataframe_operation, create_spark_context, cast_dataframe
from tabulate import tabulate

class Operator:
    def __init__(self, name: str, conn:Connection, properties: Union[dict, None], shared: dict):
        self.id = uuid.uuid4().__str__()
        self.name = name if len(name) < 30 else name[:30]
        self.conn = conn
        self.properties = properties
        self.shared = shared
    @property
    def type(self):
        return type(self).__name__
    def execute(self):
        pass

    
class ReadOperator(Operator):
    def __init__(self, name, conn, properties, shared):
        super().__init__(name, conn, properties, shared)
    def execute(self) -> DataFrame:
        sql = self.properties['sql'] if 'sql' in self.properties else None
        ops = self.properties['ops'] if 'ops' in self.properties else None
        conn = self.properties['conn'] if 'conn' in self.properties else None
        file = self.properties['file'] if 'file' in self.properties else None
        schema = self.properties['schema'] if 'fileschema' in self.properties else None

        if sql:
            df = self.conn.read(sql, conn)
        elif file:
            df = self.conn.read(file, conn)

        df = execute_dataframe_operation(df, ops) if ops else df
        df = cast_dataframe(df, schema) if schema else df

        self.properties.update({'data': df})
        return self.properties
    
class SQLOperator(Operator):
    def __init__(self, name, conn, properties, shared):
        super().__init__(name, conn, properties, shared)
    def execute(self):
        sql = self.properties['sql']
        return self.conn.sql(sql)

class WriteOperator(Operator):
    def __init__(self, name, conn, properties, shared):
        super().__init__(name, conn, properties, shared)
    def execute(self):
        ops = self.properties['ops'] if 'ops' in self.properties else None
        tablename = self.properties['tablename']
        data_opr_name = self.properties['data'].strip().lower()
        df: DataFrame = self.shared[data_opr_name]['data']
        df = execute_dataframe_operation(df, ops) if ops else df
        self.conn.write(df, tablename=tablename)

class Orchestrator:
    def __init__(self):
        self.oprs = dict()
    def add_operator(self, opr: Operator):
        if opr.name in self.oprs:
            raise NameError('Operator name must be unique')
        self.oprs.update({opr.name: opr})
    def get_operator_data(self, name: str) -> DataFrame:
        return self.oprs[name].properties['data']
    def execute(self):
        for opr in self.oprs.values():
            opr.shared.update({opr.name: opr.execute()})
    def describe(self, truncate=True):
        if truncate:
            print(
                tabulate(
                    [[orderid+1, opr.name, opr.id, opr.type, str(opr.properties) if len(str(opr.properties)) < 100 else str(opr.properties)[:100] + '...'] for orderid, opr in enumerate(self.oprs.values())],
                    headers=['order', 'name', 'id', 'type', 'properties']
                )
            )
        else:   
            print(
                tabulate(
                    [[orderid+1, opr.name, opr.id, opr.type, str(opr.properties)] for orderid, opr in enumerate(self.oprs.values())],
                    headers=['order', 'name', 'id', 'type', 'properties']
                )
            )

def create_orchestrator_by_dir(dir:str, **kwargs):
    import yaml
    from obj.connecting import CatalogConnection, JDBCConnection, FileConnection
    from jinja2 import Template, Environment, meta

    with open(dir, 'r', encoding='utf-8') as file:
        cont = file.read()
        variables = meta.find_undeclared_variables(Environment().parse(cont))
        args = dict()
        for variable in variables:
            variable = variable.strip().lower()
            
            if variable not in kwargs.keys():
                args.update({variable:input(f"{variable}: ")})
            else:
                args.update({
                    variable: kwargs[variable]
                })

        cont = Template(source=cont).render(args)
        mapping = yaml.safe_load(cont)


    orc = Orchestrator()

    shared = dict()
    for name, detail in mapping.items():
        name = name.strip().lower()
        connection = detail['connection']
        properties = detail['properties']
        operator = detail['operator']
        if connection == 'catalog':
            conn = CatalogConnection()
        elif connection == 'file':
            conn = FileConnection()
        else:
            conn = JDBCConnection(conn_id=connection)

        
        if operator == 'read':
            opr = ReadOperator(
                name=name,
                conn=conn,
                properties=properties,
                shared=shared
            )
        elif operator == 'write':
            opr = WriteOperator(
                name=name,
                conn=conn,
                properties=properties,
                shared=shared
            )
        elif operator == 'sql':
            opr = SQLOperator(
                name=name,
                conn=conn,
                properties=properties,
                shared=shared
            )
        else:
            raise Exception(f"Unknown operator {operator}")

        orc.add_operator(opr)
    
    return orc