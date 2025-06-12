from unittest import TestCase
from obj.operating import Orchestrator, ReadOperator
from obj.connecting import JDBCConnection, CatalogConnection
import unittest
class TestCatalogConnection(TestCase):
    def test_sql():
        conn = CatalogConnection()
        conn.sql('show databases').show()

conn = CatalogConnection()
conn.sql('select * from b284_cld_iceberg.iceberg_schema.dm_krd_kredirisk.partitions').show()