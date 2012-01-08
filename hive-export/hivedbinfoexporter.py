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

class DBInfo(object):
    def __init__(self, db_id, db):
        self.db_id = db_id
        self.db = db

    def __eq__(self, other):
        return self.db_id == other.db_id and self.db == other.db

class TBLInfo(object):
    def __init__(self, tbl_id, tbl, db_id):
        self.tbl_id = tbl_id
        self.tbl = tbl
        self.db_id = db_id
    
    def __eq__(self, other):
        return self.tbl_id == other.tbl_id and self.tbl == other.tbl and self.db_id == other.db_id
        
class COLInfo(object):
    def __init__(self, col_name, col_type, tbl_id):
        self.col_name = col_name
        self.col_type = col_type
        self.tbl_id = tbl_id

    def __eq__(self, other):
        return self.col_name == other.col_name and self.col_type == other.col_type and self.tbl_id == other.tbl_id

class HiveMetaData(object):
    def __init__(self):
        self.db_count = 0
        self.tbl_count = 0
        self.dbs = []
        self.tbls = []
        self.cols = []

    def get_db_id(self, db_name):
        for db_info in self.dbs:
            if db_info.db == db_name:
                return db_info.db_id
        return self.db_count

    def get_tbl_id(self, tbl_name, db_name):
        db_id = self.get_db_id(db_name)
        for tbl_info in self.tbls:
            if tbl_info.tbl == tbl_name and tbl_info.db_id == db_id:
                return tbl_info.tbl_id
        return self.tbl_count

    def is_db_exist(self, db_name):
        if get_db_id(db_name) == False:
            return True
        else:
            return False

    def is_tbl_from_db_exist(self, tbl_name, db_name):
        if get_tbl_id(tbl_name, db_name):
            return True
        else:
            return False

    def add_db(self, db_name):
        db_id = self.get_db_id(db_name)
        db_info = DBInfo(db_id, db_name)
        if not db_info in self.dbs:
            self.dbs.append(db_info)
            self.db_count += 1
    
    def add_tbl_in_db(self, tbl_name, db_name):
        db_id = self.get_db_id(db_name)
        tbl_id = self.get_tbl_id(tbl_name, db_name)
        tbl_info = TBLInfo(tbl_id, tbl_name, db_id)
        if not tbl_info in self.tbls:
            self.tbls.append(tbl_info)
            self.tbl_count += 1

    def add_col_in_tbl_of_db(self, col_name, col_type, tbl_name, db_name):
        tbl_id = self.get_tbl_id(tbl_name, db_name)
        col_info = COLInfo(col_name, col_type, tbl_id)
        if not col_info in self.cols:
            self.cols.append(col_info)

    def __eq__(self, other):
        return self.db_count == other.db_count and self.tbl_count == other.tbl_count and self.dbs == other.dbs and self.tbls == other.tbls and self.cols == other.cols

def gen_new_hive_meta_data(db_tbl_map, old_meta_data):
    meta_data = copy.deepcopy(old_meta_data)
    for db in db_tbl_map:
        meta_data.add_db(db)
        for tbl in db_tbl_map[db]:
            meta_data.add_tbl_in_db(tbl, db)
            for col_name in db_tbl_map[db][tbl]:
                meta_data.add_col_in_tbl_of_db(col_name, db_tbl_map[db][tbl][col_name], tbl, db)
    return meta_data

def print_hive_meta_data(meta_data):
    print "db info:"
    for db_info in meta_data.dbs:
        print db_info.db_id
        print db_info.db
    print "tbl info:"
    for tbl_info in meta_data.tbls:
        print tbl_info.tbl_id
        print tbl_info.tbl
        print tbl_info.db_id
    print "col info:"
    for col_info in meta_data.cols:
        print col_info.col_name + " : " + col_info.col_type
        print col_info.tbl_id

def gen_sql_list_from_hive_meta_data(meta_data):
    insert_to_db = "Insert INTO hive_dbs (db_id, db_name) VALUES (%s, '%s');"
    insert_to_tbl = "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (%s, '%s', %s);"
    insert_to_col = "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('%s', '%s', %s);"
    
    sql_list = []
    for db_info in meta_data.dbs:
        sql_list.append(insert_to_db % (db_info.db_id, db_info.db))

    for tbl_info in meta_data.tbls:
        sql_list.append(insert_to_tbl % (tbl_info.tbl_id, tbl_info.tbl, tbl_info.db_id))
    for col_info in meta_data.cols:
        sql_list.append(insert_to_col % (col_info.col_name, col_info.col_type, col_info.tbl_id))

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

        self.sql_list = ["Insert INTO hive_dbs (db_id, db_name) VALUES (0, 'log');", 
                         "Insert INTO hive_dbs (db_id, db_name) VALUES (1, 'history');", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (0, 'role', 0);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (1, 'user', 0);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (2, 'operate', 1);", 
                         "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (3, 'user', 1);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('role_name', 'string', 0);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('role_id', 'int', 0);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_name', 'string', 1);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_id', 'int', 1);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('create', 'string', 2);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('modify', 'string', 2);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_name', 'string', 3);", 
                         "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('user_id', 'int', 3);"]

        self.hive_meta_data = HiveMetaData()
        self.hive_meta_data.dbs = [DBInfo(0, "log"), DBInfo(1, "history")]
        self.hive_meta_data.db_count = 2
        self.hive_meta_data.tbls = [TBLInfo(0, "role", 0), TBLInfo(1, "user", 0),
                                    TBLInfo(2, "operate", 1), TBLInfo(3, "user", 1)]
        self.hive_meta_data.tbl_count = 4
        self.hive_meta_data.cols = [COLInfo("role_name", "string", 0), 
                                    COLInfo("role_id", "int", 0),
                                    COLInfo("user_name", "string", 1),
                                    COLInfo("user_id", "int", 1),
                                    COLInfo("create", "string", 2),
                                    COLInfo("modify", "string", 2),
                                    COLInfo("user_name", "string", 3),
                                    COLInfo("user_id", "int", 3)]

    def tearDown(self):
        pass

    def test_gen_hive_meta(self):
        expected_hive_meta = self.hive_meta_data
        hive_meta = gen_new_hive_meta_data(self.db_tbl_map, HiveMetaData())
        self.assertEqual(expected_hive_meta, hive_meta)

    def test_gen_new_hive_meta_from_old_when_db_is_added(self):
        old_map = copy.deepcopy(self.db_tbl_map)
        self.db_tbl_map["demo"] = {"client":
                                       {"client_id":"int"},
                                   "product":
                                       {"product_id":"int"}}
        expected_hive_meta = HiveMetaData()
        expected_hive_meta.dbs = [DBInfo(0, "log"), DBInfo(1, "history"),
                                  DBInfo(2, "demo")]
        expected_hive_meta.db_count = 3
        expected_hive_meta.tbls = [TBLInfo(0, "role", 0), TBLInfo(1, "user", 0),
                                   TBLInfo(2, "operate", 1), TBLInfo(3, "user", 1),
                                   TBLInfo(4, "product", 2), TBLInfo(5, "client", 2)]
        expected_hive_meta.tbl_count = 6
        expected_hive_meta.cols = [COLInfo("role_name", "string", 0), 
                                   COLInfo("role_id", "int", 0),
                                   COLInfo("user_name", "string", 1),
                                   COLInfo("user_id", "int", 1),
                                   COLInfo("create", "string", 2),
                                   COLInfo("modify", "string", 2),
                                   COLInfo("user_name", "string", 3),
                                   COLInfo("user_id", "int", 3),
                                   COLInfo("product_id", "int", 4),
                                   COLInfo("client_id", "int", 5)]
        hive_meta = gen_new_hive_meta_data(self.db_tbl_map, self.hive_meta_data)
        self.assertEqual(expected_hive_meta, hive_meta)

    def test_gen_new_hive_meta_from_old_when_db_and_tbl_are_added(self):
        old_map = copy.deepcopy(self.db_tbl_map)
        self.db_tbl_map["demo"] = {"client":
                                       {"client_id":"int"},
                                   "product":
                                       {"product_id":"int"}}
        self.db_tbl_map["log"]["level"] = {"level_id":"int"}
        self.db_tbl_map["history"]["operate"]["delete"] = "string"

        expected_hive_meta = HiveMetaData()
        expected_hive_meta.dbs = [DBInfo(0, "log"), DBInfo(1, "history"),
                                  DBInfo(2, "demo")]
        expected_hive_meta.db_count = 3
        expected_hive_meta.tbls = [TBLInfo(0, "role", 0), TBLInfo(1, "user", 0), 
                                   TBLInfo(2, "operate", 1), TBLInfo(3, "user", 1),
                                   TBLInfo(4, "product", 2), TBLInfo(5, "client", 2),
                                   TBLInfo(6, "level", 0)]
        expected_hive_meta.tbl_count = 7
        expected_hive_meta.cols = [COLInfo("role_name", "string", 0), 
                                   COLInfo("role_id", "int", 0),
                                   COLInfo("user_name", "string", 1),
                                   COLInfo("user_id", "int", 1),
                                   COLInfo("create", "string", 2),
                                   COLInfo("modify", "string", 2),
                                   COLInfo("user_name", "string", 3),
                                   COLInfo("user_id", "int", 3),
                                   COLInfo("product_id", "int", 4),
                                   COLInfo("client_id", "int", 5),
                                   COLInfo("level_id", "int", 6),
                                   COLInfo("delete", "string", 2)]
        hive_meta = gen_new_hive_meta_data(self.db_tbl_map, self.hive_meta_data)
        self.assertEqual(expected_hive_meta, hive_meta)

    def test_gen_sql_list_from_hive_meta_data(self):
        sql_list = gen_sql_list_from_hive_meta_data(self.hive_meta_data)
        self.assertEqual(self.sql_list, sql_list)
        
    # def test_fetch_hive_info(self):
    #     fetch_db_info_from_hive("localhost")

    def test_save_db_tbl_map(self):
        path = "db_tbl.map"
        db_tbl_map = self.db_tbl_map
        save_db_tbl_map(db_tbl_map, path)
        db_tbl_map_new = load_db_tbl_map(path)

        self.assertEqual(db_tbl_map, db_tbl_map_new)
        os.remove(path)


if __name__ == "__main__":
    unittest.main()
