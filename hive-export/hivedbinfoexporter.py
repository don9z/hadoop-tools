#!/usr/bin/env python
import sys
sys.path.append("/Users/chris/Workspace/hadoop/hive-0.7.1-cdh3u2/lib/py")

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def fetch_db_info_from_hive(hive_server_addr):
    """
    Fetches the databases, tables, and columns from hive.

    Args:
        `hive_server_addr`: address of hive server

    Returns:
        A map contains
          {database : {table : {column_name : column_type, ...}, ...}, ...}
    """
    try:
        transport = TSocket.TSocket(hive_server_addr, 10000)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        client = ThriftHive.Client(protocol)
        transport.open()
        
        # Fetch databases
        client.execute("show databases")
        dbs = client.fetchAll()

        # Fetch tables
        db_tbl_map = {}
        for db in dbs:
            client.execute("use " + db)
            client.execute("show tables")
            tbls = client.fetchAll()
            
            tbl_col_map = {}
            for tbl in tbls:
                col_map = {}

                # Fetch table column name and type
                client.execute("describe " + tbl)
                cols = client.fetchAll()
                
                for col in cols:
                    words = col.split()
                    col_map[words[0]] = words[1]

                tbl_col_map[tbl] = col_map;
            db_tbl_map[db] = tbl_col_map;
        
        transport.close()
        return db_tbl_map

    except Thrift.TException, tx:
        print '%s' % (tx.message)


def generate_sql_from_export(hive_server_addr):
    insert_to_db = "Insert INTO hive_dbs (db_id, db_name) VALUES (%s, '%s');"
    insert_to_tbl = "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (%s, '%s', %s);"
    insert_to_col = "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (%s, '%s', '%s', %s);"
    db_tbl_map = fetch_db_info_from_hive(hive_server_addr)

    db_count = 0;
    for db in db_tbl_map:
        print insert_to_db % (db_count, db)
        tbl_col_map = db_tbl_map[db]

        tbl_count = 0;
        for tbl in tbl_col_map:
            print insert_to_tbl % (tbl_count, tbl, db_count)
            col_map = tbl_col_map[tbl]

            col_count = 0;
            for (col_name, col_type) in col_map.items():
                print insert_to_col % (col_count, col_name, col_type, tbl_count)
                col_count += 1;
            tbl_count+=1;
        db_count+=1;


# Test
import unittest

class TestHiveExporter(unittest.TestCase):

    def test_sql_generate(self):
        generate_sql_from_export("hz169-98.i.netease.com")

if __name__ == "__main__":
    unittest.main()
