#!/usr/bin/env python
import sys
# Add thrift path to PYTHONPATH
sys.path.append("/Users/chris/Workspace/hadoop/hive-0.7.1-cdh3u2/lib/py")

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import pickle

def fetch_db_info_from_hive(hive_server_addr):
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


def generate_sql_from_map(db_tbl_map):
    insert_to_db = "Insert INTO hive_dbs (db_id, db_name) VALUES (%s, '%s');"
    insert_to_tbl = "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (%s, '%s', %s);"
    insert_to_col = "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (%s, '%s', '%s', %s);"
    
    sql_list = []
    db_sql_list = []
    tbl_sql_list = []
    col_sql_list = []

    db_count = 0;
    for db in db_tbl_map:
        db_sql_list.append(insert_to_db % (db_count, db))
        tbl_col_map = db_tbl_map[db]

        tbl_count = 0;
        for tbl in tbl_col_map:
            tbl_sql_list.append(insert_to_tbl % (tbl_count, tbl, db_count))
            col_map = tbl_col_map[tbl]

            col_count = 0;
            for (col_name, col_type) in col_map.items():
                col_sql_list.append(insert_to_col % (col_count, col_name, col_type, tbl_count))
                col_count += 1;
            tbl_count+=1;
        db_count+=1;
    sql_list.extend(db_sql_list)
    sql_list.extend(tbl_sql_list)
    sql_list.extend(col_sql_list)
    return sql_list

def save_db_tbl_map(db_tbl_map, file_path):
    output = open(file_path, "wb")
    pickle.dump(db_tbl_map, output)
    output.close()

def load_db_tbl_map(file_path):
    map_file = open(file_path, "rb")
    db_tbl_map = pickle.load(map_file)
    map_file.close()
    return db_tbl_map

# Test
import unittest
import os

class TestHiveExporter(unittest.TestCase):

    def setUp(self):
        self.test_map = {"log":
                             {"user":
                                  {"user_id":"int",
                                   "user_name":"string"},
                              "role":
                                  {"role_id":"int",
                                   "role_name":"string"}
                              },
                         "history":
                             {"user":
                                  {"user_id":"int",
                                   "user_name":"string"},
                              "operate":
                                  {"modify":"string",
                                   "create":"string"}
                              }
                         }

        self.sql_list = ["Insert INTO hive_dbs (db_id, db_name) VALUES (0, 'log');", 
                         "Insert INTO hive_dbs (db_id, db_name) VALUES (1, 'history');", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'role', 0);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'user', 0);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'operate', 1);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'user', 1);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (0, 'role_name', 'string', 0);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (1, 'role_id', 'int', 0);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (0, 'user_name', 'string', 1);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (1, 'user_id', 'int', 1);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (0, 'create', 'string', 0);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (1, 'modify', 'string', 0);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (0, 'user_name', 'string', 1);", 
                         "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (1, 'user_id', 'int', 1);"]

    def tearDown(self):
        pass

    # def test_fetch_hive_info(self):
    #     fetch_db_info_from_hive("localhost")

    def test_sql_generate(self):
        sql_list = generate_sql_from_map(self.test_map)
        self.assertEqual(self.sql_list, sql_list)

    def test_save_db_tbl_map(self):
        path = "db_tbl.map"
        db_tbl_map = self.test_map
        save_db_tbl_map(db_tbl_map, path)
        db_tbl_map_new = load_db_tbl_map(path)
        self.assertEqual(db_tbl_map, db_tbl_map_new)
        os.remove(path)


if __name__ == "__main__":
    unittest.main()
