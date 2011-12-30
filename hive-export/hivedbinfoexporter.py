#!/usr/bin/env python

import sys
sys.path.append("/Users/chris/Workspace/hadoop/hive-0.7.1-cdh3u2/lib/py")

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    transport = TSocket.TSocket("localhost", 10000)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = ThriftHive.Client(protocol)
    transport.open()
    
    # Fetch databases
    client.execute("show databases")
    dbs = client.fetchAll()

    # Fetch tables
    dbTblMap = {}
    for db in dbs:
        client.execute("use " + db)
        client.execute("show tables")
        tbls = client.fetchAll()
        
        tblColMap = {}
        for tbl in tbls:
            colMap = {}

            # Fetch table column name and type
            client.execute("describe " + tbl)
            cols = client.fetchAll()
            
            for col in cols:
                words = col.split()
                colMap[words[0]] = words[1]
            
            tblColMap[tbl] = colMap;

        dbTblMap[db] = tblColMap;
    
    dbCount = 0;
    for db in dbTblMap:
        print "Insert INTO hive_dbs (db_id, db_name) VALUES (%s, %s);" % (dbCount, db)
        tblMap = dbTblMap[db]

        tblCount = 0;
        for tbl in tblColMap:
            print "Insert INTO hive_tbls (tbl_id, tbl_name, db_id) VALUES (%s, %s, %s);" % (tblCount, tbl, dbCount)
            colMap = tblColMap[tbl]

            colCount = 0;
            for (col_name, col_type) in colMap.items():
                print "Insert INTO hive_cols (col_id, col_name, col_type, tbl_id) VALUES (%s, %s, %s, %s);" % (colCount, col_name, col_type, tblCount)
                colCount += 1;
            tblCount+=1;
        dbCount+=1;

    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)
