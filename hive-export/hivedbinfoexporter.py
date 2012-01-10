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

def fetch_db_info_from_hive(hive_server_addr, port=10000):
    try:
        transport = TSocket.TSocket(hive_server_addr, port)
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

def print_db_info(db_tbl_map):
    for db in db_tbl_map:
        print db
        for tbl in db_tbl_map[db]:
            print "    " + tbl
            for col in db_tbl_map[db][tbl]:
                print "        " + col.ljust(12), db_tbl_map[db][tbl][col]

class DBInfo(object):
    def __init__(self, db_id, db_name):
        self.db_id = db_id
        self.db_name = db_name

    def __eq__(self, other):
        return self.db_id == other.db_id and self.db_name == other.db_name

class TBLInfo(object):
    def __init__(self, tbl_id, tbl_name, db_id):
        self.tbl_id = tbl_id
        self.tbl_name = tbl_name
        self.db_id = db_id
    
    def __eq__(self, other):
        return self.tbl_id == other.tbl_id and self.tbl_name == other.tbl_name and self.db_id == other.db_id
        
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
            if db_info.db_name == db_name:
                return db_info.db_id
        return self.db_count

    def get_tbl_id(self, tbl_name, db_name):
        db_id = self.get_db_id(db_name)
        for tbl_info in self.tbls:
            if tbl_info.tbl_name == tbl_name and tbl_info.db_id == db_id:
                return tbl_info.tbl_id
        return self.tbl_count

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

def gen_new_hive_meta_data(db_tbl_map, old_meta_data=HiveMetaData()):
    meta_data = copy.deepcopy(old_meta_data)
    for db in db_tbl_map:
        meta_data.add_db(db)
        for tbl in db_tbl_map[db]:
            meta_data.add_tbl_in_db(tbl, db)
            for col_name in db_tbl_map[db][tbl]:
                meta_data.add_col_in_tbl_of_db(col_name, db_tbl_map[db][tbl][col_name], tbl, db)
    return meta_data

def print_hive_meta_data(meta_data):
    print "-------- db info --------"
    print "db_id".ljust(8), "db_name".ljust(20)
    for db_info in meta_data.dbs:
        print repr(db_info.db_id).ljust(8), db_info.db_name.ljust(20)
    print "-------- tbl info --------"
    print "tbl_id".ljust(8), "db_id".ljust(8), "tbl_name".ljust(20)
    for tbl_info in meta_data.tbls:
        print repr(tbl_info.tbl_id).ljust(8), repr(tbl_info.db_id).ljust(8), tbl_info.tbl_name.ljust(20)
    print "-------- col info --------"
    print "col_id".ljust(8), "col_name".ljust(15), "col_type".ljust(10)
    for col_info in meta_data.cols:
        print repr(col_info.tbl_id).ljust(8), col_info.col_name.ljust(15), col_info.col_type.ljust(10)

def gen_sql_list_from_hive_meta_data(meta_data):
    insert_to_db = "Insert INTO hive_dbs (db_id, db_name) VALUES (%s, '%s');"
    insert_to_tbl = "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (%s, '%s', %s);"
    insert_to_col = "Insert INTO hive_cols (col_name, col_type, tbl_id) VALUES ('%s', '%s', %s);"
    
    sql_list = []
    for db_info in meta_data.dbs:
        sql_list.append(insert_to_db % (db_info.db_id, db_info.db_name))

    for tbl_info in meta_data.tbls:
        sql_list.append(insert_to_tbl % (tbl_info.tbl_id, tbl_info.tbl_name, tbl_info.db_id))
    for col_info in meta_data.cols:
        sql_list.append(insert_to_col % (col_info.col_name, col_info.col_type, col_info.tbl_id))
    return sql_list

def print_sql_list(sql_list):
    for sql in sql_list:
        print sql

def save_hive_meta_data(hive_meta_data, file_path):
    output = open(file_path, "wb")
    pickle.dump(hive_meta_data, output)
    output.close()

def load_hive_meta_data(file_path):
    map_file = open(file_path, "rb")
    hive_meta_data = pickle.load(map_file)
    map_file.close()
    return hive_meta_data

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

    # def test_fetch_hive_info(self):
    #     fetch_db_info_from_hive("localhost")

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
        expected_hive_meta = copy.deepcopy(self.hive_meta_data)
        expected_hive_meta.dbs.append(DBInfo(2, "demo"))
        expected_hive_meta.db_count = 3
        expected_hive_meta.tbls.extend([TBLInfo(4, "product", 2), TBLInfo(5, "client", 2)])
        expected_hive_meta.tbl_count = 6
        expected_hive_meta.cols.extend([COLInfo("product_id", "int", 4),
                                       COLInfo("client_id", "int", 5)])
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
        expected_hive_meta = copy.deepcopy(self.hive_meta_data)
        expected_hive_meta.dbs.append(DBInfo(2, "demo"))
        expected_hive_meta.db_count = 3
        expected_hive_meta.tbls.extend([TBLInfo(4, "product", 2), 
                                        TBLInfo(5, "client", 2),
                                        TBLInfo(6, "level", 0)])
        expected_hive_meta.tbl_count = 7
        expected_hive_meta.cols.extend([COLInfo("product_id", "int", 4),
                                        COLInfo("client_id", "int", 5),
                                        COLInfo("level_id", "int", 6),
                                        COLInfo("delete", "string", 2)])
        hive_meta = gen_new_hive_meta_data(self.db_tbl_map, self.hive_meta_data)
        self.assertEqual(expected_hive_meta, hive_meta)

    def test_gen_sql_list_from_hive_meta_data(self):
        sql_list = gen_sql_list_from_hive_meta_data(self.hive_meta_data)
        self.assertEqual(self.sql_list, sql_list)
        
    def test_save_hive_meta_data(self):
        path = "hive_meta.data"
        hive_meta_data = copy.deepcopy(self.hive_meta_data)
        save_hive_meta_data(hive_meta_data, path)
        new_hive_meta_data = load_hive_meta_data(path)
        self.assertEqual(self.hive_meta_data, new_hive_meta_data)
        os.remove(path)

import argparse

def get_sqls(args):
    ns = vars(args)
    url = ns["from"].split(":")
    if len(url) == 2:
        db_tbl_map = fetch_db_info_from_hive(url[0], url[1])
    else:
        db_tbl_map = fetch_db_info_from_hive(url[0])
    hive_meta_data = gen_new_hive_meta_data(db_tbl_map, HiveMetaData())
    save_hive_meta_data(hive_meta_data, ns["out"])
    print_sql_list(gen_sql_list_from_hive_meta_data(hive_meta_data))

def update_sqls(args):
    ns = vars(args)
    url = ns["from"].split(":")
    if len(url) == 2:
        db_tbl_map = fetch_db_info_from_hive(url[0], url[1])
    else:
        db_tbl_map = fetch_db_info_from_hive(url[0])
    old_meta_data = load_hive_meta_data(ns["in"])
    hive_meta_data = gen_new_hive_meta_data(db_tbl_map, old_meta_data)
    print_sql_list(gen_sql_list_from_hive_meta_data(hive_meta_data))
    save_hive_meta_data(hive_meta_data, ns["out"])

def show_hive_db_info(args):
    ns = vars(args)
    if ns["from"]:
        url = ns["from"].split(":")
        if len(url) == 2:
            db_tbl_map = fetch_db_info_from_hive(url[0], url[1])
        else:
            db_tbl_map = fetch_db_info_from_hive(url[0])
        print_db_info(db_tbl_map)

    if ns["path"]:
        print_hive_meta_data(load_hive_meta_data(ns["path"]))

def main():
    parser = argparse.ArgumentParser(description="Get or update sql list from hive server")
    subparsers = parser.add_subparsers(help="sub-command help")

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("-f", "--from", required=True, metavar="url[:port]", help="hive server address")
    parent_parser.add_argument("-o", "--out", required=True, metavar="path", help="backup file (save for updating sql)")
    
    parser_get = subparsers.add_parser("get", parents=[parent_parser], help="get sqls from hive server")
    parser_get.set_defaults(func=get_sqls)

    parser_update = subparsers.add_parser("update", parents=[parent_parser], help="udpate sqls from hive server")
    parser_update.add_argument("-i", "--in", required=True, metavar="path", help="previous backup file")
    parser_update.set_defaults(func=update_sqls)

    parser_show = subparsers.add_parser("show", help="show hive db info from hive server, or from meta data file")
    group = parser_show.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", "--from", help="hive server address")
    group.add_argument("-p", "--path", help="previous backup file")
    parser_show.set_defaults(func=show_hive_db_info)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    sys.exit(main())
#    unittest.main()
