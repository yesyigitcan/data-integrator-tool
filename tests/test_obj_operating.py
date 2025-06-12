from unittest import TestCase
from obj.operating import Orchestrator, ReadOperator
from obj.connecting import JDBCConnection
import unittest
class TestOrchestrator(TestCase):
    def test_add_operator(self):
        orc = Orchestrator()
        opr = ReadOperator(
                name='same_name',
                conn=JDBCConnection(conn_id='test'),
                properties=None
            )
        
        orc.add_operator(opr)
        self.assertEqual(orc.oprs[0], opr)

        with self.assertRaises(NameError):
            orc.add_operator(opr)

if __name__ == '__main__':
    unittest.main()