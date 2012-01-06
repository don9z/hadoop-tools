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
import copy

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

def generate_sql_list_from_map_by_order(db_tbl_map, db_tbl_order_list):
    insert_to_db = "Insert INTO hive_dbs (db_id, db_name) VALUES (%s, '%s');"
    insert_to_tbl = "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (%s, '%s', %s);"
    insert_to_col = "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('%s', '%s', %s);"
    
    sql_list = []
    db_sql_list = []
    tbl_sql_list = []
    col_sql_list = []

    db_count = 0
    for order_map in db_tbl_order_list:
        for db in order_map:
            db_sql_list.append(insert_to_db % (db_count, db))
            tbl_count = 0
            for tbl in order_map[db]:
                tbl_sql_list.append(insert_to_tbl % (tbl_count, tbl, db_count))
                for col_name in db_tbl_map[db][tbl]:
                    col_sql_list.append(insert_to_col % (col_name, db_tbl_map[db][tbl][col_name], tbl_count))
                tbl_count += 1
        db_count += 1
    sql_list.append(db_sql_list)
    sql_list.append(tbl_sql_list)
    sql_list.append(col_sql_list)
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

def generate_order_list(db_tbl_map, old_order_list):
    new_order_list = copy.deepcopy(old_order_list)
    if not old_order_list:
        for db in db_tbl_map:
            tbl_list = []
            for tbl in db_tbl_map[db]:
                tbl_list.append(tbl)
            new_order_list.append({db:tbl_list})
    else:
        db_index = 0
        for db in db_tbl_map:
            is_new_db = True
            for db_order in old_order_list:
                if (db == db_order.keys()[0]):
                    is_new_db = False
            if is_new_db:
                tbl_list = [];
                for tbl in db_tbl_map[db]:
                    tbl_list.append(tbl)
                db_tbl = {db:tbl_list}
                new_order_list.append(db_tbl)
                continue
            
            for tbl in db_tbl_map[db]:
                is_new_tbl = True
                for tbl_order in new_order_list[db_index][db]:
                    if (tbl == tbl_order):
                        is_new_tbl = False
                if is_new_tbl:
                    new_order_list[db_index][db].append(tbl)
            db_index += 1
    return new_order_list

# Test
import unittest
import os

class TestHiveExporter(unittest.TestCase):

    def setUp(self):
        self.db_tbl_map = {"log":
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

        self.sql_list = [["Insert INTO hive_dbs (db_id, db_name) VALUES (0, 'log');", 
                         "Insert INTO hive_dbs (db_id, db_name) VALUES (1, 'history');"], 
                         ["Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'role', 0);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'user', 0);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'operate', 1);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'user', 1);"], 
                         ["Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('role_name', 'string', 0);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('role_id', 'int', 0);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_name', 'string', 1);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_id', 'int', 1);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('create', 'string', 0);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('modify', 'string', 0);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_name', 'string', 1);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_id', 'int', 1);"]]

        self.db_tbl_order_list = [{"log":["role", "user"]},
                                  {"history":["operate", "user"]}]

        self.sql_list_after_add_dbs_and_tbls = [["Insert INTO hive_dbs (db_id, db_name) VALUES (0, 'log');", 
                                                 "Insert INTO hive_dbs (db_id, db_name) VALUES (1, 'history');",
                                                 "Insert INTO hive_dbs (db_id, db_name) VALUES (2, 'demo2');",
                                                 "Insert INTO hive_dbs (db_id, db_name) VALUES (3, 'demo1');"], 
                                                ["Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'role', 0);", 
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'user', 0);", 
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (2, 'client', 0);", 
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'operate', 1);", 
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'user', 1);",
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (2, 'date', 1);",
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'feature', 2);",
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'agent', 2);",
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'product', 3);"
                                                 "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'client', 3);"], 
                                                ["Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('role_name', 'string', 0);", 
                                                 "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('role_id', 'int', 0);", 
                                                 "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_name', 'string', 1);", 
                                                 "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_id', 'int', 1);", 
                                                 
                                                 "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('create', 'string', 0);", 
                                                 "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('modify', 'string', 0);", 
                                                 "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_name', 'string', 1);", 
                                                 "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_id', 'int', 1);"]]

    def tearDown(self):
        pass

    # def test_fetch_hive_info(self):
    #     fetch_db_info_from_hive("localhost")

    def test_generate_order_list_first_time(self):
        old_order_list = []
        new_order_list = generate_order_list(self.db_tbl_map, old_order_list)

        self.assertEqual(self.db_tbl_order_list, new_order_list)

    def test_generate_order_list_when_db_is_added(self):
        db_tbl_map = self.db_tbl_map
        db_tbl_map["demo"] = {"client":
                                  {"client_id":"int",
                                   "client_name":"string"},
                              "product":
                                  {"product_id":"int",
                                   "product_name":"string"}}
        expected_order_list = copy.deepcopy(self.db_tbl_order_list)
        expected_order_list.append({"demo":["product", "client"]})
        
        old_order_list = self.db_tbl_order_list
        new_order_list = generate_order_list(db_tbl_map, old_order_list)

        self.assertEqual(expected_order_list, new_order_list)

    def test_generate_order_list_when_tbl_is_added(self):
        db_tbl_map = self.db_tbl_map
        db_tbl_map["log"]["client"] = {"client_id":"int",
                                       "client_name":"string"}

        expected_order_list = copy.deepcopy(self.db_tbl_order_list)
        expected_order_list[0]["log"].append("client")

        new_order_list = generate_order_list(db_tbl_map, self.db_tbl_order_list)

        self.assertEqual(expected_order_list, new_order_list)

    def test_generate_order_list_without_affect_old_order_list(self):
        db_tbl_map = self.db_tbl_map
        db_tbl_map["log"]["client"] = {"client_id":"int",
                                       "client_name":"string"}
        old_order_list = self.db_tbl_order_list
        expected_old_order_list = copy.deepcopy(self.db_tbl_order_list)

        generate_order_list(db_tbl_map, old_order_list)

        self.assertEqual(expected_old_order_list, old_order_list)
        
    def test_generate_order_list_when_multi_tbls_are_added(self):
        db_tbl_map = self.db_tbl_map
        db_tbl_map["log"]["client"] = {"client_id":"int",
                                       "client_name":"string"}
        db_tbl_map["history"]["date"] = {"year":"int",
                                         "month":"int",
                                         "day":"int"}
        expected_order_list = copy.deepcopy(self.db_tbl_order_list)
        expected_order_list[0]["log"].append("client")
        expected_order_list[1]["history"].append("date")

        new_order_list = generate_order_list(db_tbl_map, self.db_tbl_order_list)

        self.assertEqual(expected_order_list, new_order_list)

    def test_generate_order_list_when_multi_tbls_and_dbs_are_added(self):
        db_tbl_map = self.db_tbl_map
        db_tbl_map["log"]["client"] = {"client_id":"int",
                                       "client_name":"string"}
        db_tbl_map["history"]["date"] = {"year":"int",
                                         "month":"int",
                                         "day":"int"}
        db_tbl_map["demo1"] = {"client":
                                  {"client_id":"int",
                                   "client_name":"string"},
                              "product":
                                  {"product_id":"int",
                                   "product_name":"string"}}
        db_tbl_map["demo2"] = {"agent":
                                  {"agent_id":"int",
                                   "agent_name":"string"},
                              "feature":
                                  {"feature_id":"int",
                                   "feature_name":"string"}}
        expected_order_list = copy.deepcopy(self.db_tbl_order_list)
        expected_order_list[0]["log"].append("client")
        expected_order_list[1]["history"].append("date")
        expected_order_list.append({"demo2":["feature", "agent"]})
        expected_order_list.append({"demo1":["product", "client"]})

        new_order_list = generate_order_list(db_tbl_map, self.db_tbl_order_list)

        self.assertEqual(expected_order_list, new_order_list)

    def test_save_db_tbl_map(self):
        path = "db_tbl.map"
        db_tbl_map = self.db_tbl_map
        save_db_tbl_map(db_tbl_map, path)
        db_tbl_map_new = load_db_tbl_map(path)

        self.assertEqual(db_tbl_map, db_tbl_map_new)
        os.remove(path)


if __name__ == "__main__":
    unittest.main()
