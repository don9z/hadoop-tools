#!/usr/bin/env python
import sys
import os
import re
# Add thrift path to PYTHONPATH
sys.path.append("/Users/chris/Workspace/hadoop/hive-0.7.1-cdh3u2/lib/py")

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def is_path_valid(path, table_name):
    regex = '^/(.+/)*' + table_name + '(/[^/=]+=[^/=]+)*/?$'
    if re.match(regex, path) == None:
        return False
    else:
        return True

def get_partitions_from_path(path, table_name):
    partitions = []
    name_pair = os.path.split(path)
    while name_pair[1] != table_name:
        # Ignore the last / in path
        if name_pair[1] != '':
            partitions.append(name_pair[1]);
        name_pair = os.path.split(name_pair[0])
    return partitions

def generate_alter_sql(partitions, table_name):
    par_count = len(partitions)
    if (par_count == 0):
        return None

    par_string = ''
    while (par_count > 0):
        par_string += partitions[par_count-1]
        if par_count > 1:
            par_string += ','
        par_count = par_count - 1
    return 'ALTER TABLE ' + table_name + ' ADD PARTITION(' + par_string +')'

def execute_alter_sql(sql, hive_server_addr, port=10000):
    try:
        transport = TSocket.TSocket(hive_server_addr, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        client = ThriftHive.Client(protocol)
        transport.open()
        
        # Fetch databases
        client.execute(sql)

    except Thrift.TException, tx:
        print '%s' % (tx.message)

import argparse

def add_partitions_to_hive(args):
    ns = vars(args)
    path = ns["path"]
    table_name = ns["table_name"]
    url = ns["from"].split(":")
    server_ip = '127.0.0.1'
    server_port = 10000
    if len(url) == 2:
        server_ip = url[0]
        server_port = url[1]
    else:
        server_ip = url[0]

    if (not is_path_valid(path, table_name)):
        print "HDFS path is not valid, example '/user/user_name/table_name/p1=2/p2=3'"
        return
    
    sql = generate_alter_sql(get_partitions_from_path(path, table_name), table_name)
    if (sql != None):
        print sql
        execute_alter_sql(sql, server_ip, server_port)
    else:
        print 'No need to add partitions'

def main():
    parser = argparse.ArgumentParser(description="Add partitions to hive by path")
    parser.add_argument("-p", "--path", required=True, metavar="path", 
                        help="Hive table HDFS path, example: /user/user_name/table_name/p1=2/p2=3")
    parser.add_argument("-n", "--name", required=True, metavar="name", 
                        help="Hive table name")
    parser.add_argument("-s", "--server", required=True, metavar="url[:port]", 
                        help="hive server address")
    parser.set_defaults(func=add_partitions_to_hive)

    args = parser.parse_args()
    args.func(args)

#Test
import unittest

class TestAddPartitionByPath(unittest.TestCase):

    def test_generate_alter_sql(self):
        sql = generate_alter_sql(["server=1", "ds=2012-02-23"], "user_info")
        self.assertEqual("ALTER TABLE user_info ADD PARTITION(ds=2012-02-23,server=1)",
                         sql)
    
    def test_get_partitions_from_path(self):
        partitions = get_partitions_from_path(
            "/user/da/user_info/ds=2010-02-23/server=1", "user_info")
        self.assertEqual(2, len(partitions))
        self.assertEqual("ds=2010-02-23", partitions[1])
        self.assertEqual("server=1", partitions[0])

    def test_get_partitions_from_path_with_slash(self):
        partitions = get_partitions_from_path(
            "/user/da/user_info/ds=2010-02-23/server=1/", "user_info")
        self.assertEqual(2, len(partitions))
        self.assertEqual("ds=2010-02-23", partitions[1])
        self.assertEqual("server=1", partitions[0])

    def test_get_partitions_from_path_table_name_as_root(self):
        partitions = get_partitions_from_path(
            "/user_info", "user_info")
        self.assertEqual(0, len(partitions))

    def test_get_partitions_from_path_table_name_as_root_with_partitions(self):
        partitions = get_partitions_from_path(
            "/user_info/ds=2010-02-23", "user_info")
        self.assertEqual(1, len(partitions))
        self.assertEqual("ds=2010-02-23", partitions[0])
        
    def test_path_is_invalid_not_include_table_name(self):
        self.assertFalse(is_path_valid("/user/da/info/ds=2010-02-23/server=1/", 
                                       "user_info"))

    def test_path_is_invalid_not_include_equal_sign_after_table_name(self):
        self.assertFalse(is_path_valid("/user/da/user_info/ds/server=1/", 
                                       "user_info"))

    def test_path_is_invalid_not_from_root(self):
        self.assertFalse(is_path_valid("user/da/user_info/ds/server=1/", 
                                       "user_info"))

    def test_path_is_valid_only_table_name(self):
        self.assertTrue(is_path_valid("/user/da/user_info", "user_info"))

    def test_path_is_valid_only_table_name_in_root(self):
        self.assertTrue(is_path_valid("/user_info", "user_info"))

    def test_path_is_valid_table_name_with_partitions(self):
        self.assertTrue(is_path_valid("/user/da/user_info/ds=2012-02-03/server=1", 
                                      "user_info"))

    def test_path_is_valid_ignore_last_slash(self):
        self.assertTrue(is_path_valid("/user/da/user_info/ds=2012-02-03/server=1", 
                                      "user_info"))
        self.assertTrue(is_path_valid("/user/da/user_info/ds=2012-02-03/server=1/", 
                                      "user_info"))

if __name__ == "__main__":
#    unittest.main()
    sys.exit(main())
