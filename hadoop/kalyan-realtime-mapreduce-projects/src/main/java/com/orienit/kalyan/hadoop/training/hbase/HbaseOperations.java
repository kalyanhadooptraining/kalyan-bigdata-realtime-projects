package com.orienit.kalyan.hadoop.training.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOperations {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		try {
			Admin admin = connection.getAdmin();
			createHbaseTable(admin, "input", "cf");
			createHbaseTable(admin, "output", "cf");

			TableName tableName = TableName.valueOf("input");
			Table table = connection.getTable(tableName);
			try {
				byte[] row1 = Bytes.toBytes("row1");
				byte[] row2 = Bytes.toBytes("row2");
				byte[] row3 = Bytes.toBytes("row3");
				byte[] row4 = Bytes.toBytes("row4");

				byte[] cf = Bytes.toBytes("cf");
				byte[] q = Bytes.toBytes("record");

				byte[] value1 = Bytes.toBytes("I am going");
				byte[] value2 = Bytes.toBytes("to hyd");
				byte[] value3 = Bytes.toBytes("I am learning");
				byte[] value4 = Bytes.toBytes("hadoop course");

				// put operation
				Put p = new Put(row1);
				p.add(cf, q, value1);
				table.put(p);

				p = new Put(row2);
				p.add(cf, q, value2);
				table.put(p);

				p = new Put(row3);
				p.add(cf, q, value3);
				table.put(p);

				p = new Put(row4);
				p.add(cf, q, value4);
				table.put(p);

				// scan operation
				Scan s = new Scan();
				s.addColumn(cf, q);
				ResultScanner scanner = table.getScanner(s);
				try {
					for (Result rr : scanner) {
						System.out.println("Found row: " + rr);
					}
				} finally {
					scanner.close();
				}
			} finally {
				if (table != null)
					table.close();
			}
		} finally {
			connection.close();
		}
	}

	private static void createHbaseTable(Admin admin, String tableName, String columnFamilyName) throws IOException {
		TableName tblName = TableName.valueOf(tableName);
		boolean tableExists = admin.tableExists(tblName);
		if (tableExists) {
			admin.disableTable(tblName);
			admin.deleteTable(tblName);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tblName);
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamilyName);
		tableDescriptor.addFamily(columnDescriptor);
		admin.createTable(tableDescriptor);
	}
}
