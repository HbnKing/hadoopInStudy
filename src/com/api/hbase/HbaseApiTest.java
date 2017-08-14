package com.api.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService.AsyncProcessor.put;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.stringLiteralSequence_return;


import org.apache.hadoop.hbase.util.Bytes;


public class HbaseApiTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//createtable("members");
		//insertTableByput("member");
		//queryByGet("member");
		// queryByScan("member");
		deleteData("member");
	}
	
	public  static Configuration  conf;
	static {
		conf = HBaseConfiguration.create();
		//设置zookeeper
		conf.set("hbase.zookeeper.quorum", "sla1,sla2,sla3");
		//配置端口号
		conf.set("hbase.zookeeper.property.clientport", "2181");
		//配置master  这里配置的是客户端的接口   端口号新版为16010  旧版为60000  
		//新版本可以不用配置 hbasemaster
		//conf.set("hbasemaster", "sla0:16010");
	}
	
	public static void createtable(String tablename) throws IOException{
		
		@SuppressWarnings("deprecation")
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		//判断表是否存在  ,存在就删除
		if(hBaseAdmin.tableExists(tablename)){
			hBaseAdmin.disableTable(tablename);
			hBaseAdmin.deleteTable(tablename);
		}
		//对表的一些信息进行描述. 表的列名等
		@SuppressWarnings("deprecation")
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tablename);
		hTableDescriptor.addFamily(new HColumnDescriptor("info"));
		hTableDescriptor.addFamily(new HColumnDescriptor("address"));
		
		//创建表
		hBaseAdmin.createTable(hTableDescriptor);
		//释放资源
		hBaseAdmin.close();
	}
	@SuppressWarnings({ "deprecation", "deprecation", "resource" })
	public static void insertTableByput(String tablename) throws IOException{
		
		
		HTable hTable = new HTable(conf, tablename);
		Put put1 = new Put(getBytes("djt"));
	
		put1.add(getBytes("address"), getBytes("country"), getBytes("china"));
		put1.add(getBytes("address"), getBytes("provice"),getBytes("hubei"));
		put1.add(getBytes("address"), getBytes("city"), getBytes("wuhan"));
		
		put1.add(getBytes("info"), getBytes("age"), getBytes("18"));
		put1.add(getBytes("info"), getBytes("birthday"), getBytes("2017-04-01"));
		put1.add(getBytes("info"), getBytes("company"), getBytes("dajiangtai"));
		
		//插入数据
		hTable.put(put1);
		//释放资源
		hTable.close();
	}
	
	@SuppressWarnings("deprecation")
	public static void queryByGet(String tablename) throws IOException {
		HTable hTable = new HTable(conf, tablename);
		Get get = new Get(getBytes("djt")); //根据rowkey 进行查询
		Result rsl = hTable.get(get);
		//输入获取到的rowkey
		System.out.println( "获取到的rowkey"+new String(rsl.getRow()));
		for (KeyValue keyValue : rsl.raw()) {
			System.out.println("列簇"+new String(keyValue.getFamily())+"===列:"+new String(keyValue.getQualifier())+"===值"+new String(keyValue.getValue()));
			
		}
		
		hTable.close();
	}
	
	
	public static void queryByScan(String tablename) throws IOException{
		HTable hTable = new HTable(conf, tablename);
	
		Scan scan = new Scan();
		//添加指定扫描内容
		//scan.addColumn(getBytes("address"), getBytes("city"));
		ResultScanner scanner = hTable.getScanner(scan);
		//遍历输出scanner
		for(Result rs:scanner){
			System.out.println("获取到的rowkey:"+new String(rs.getRow()));
			//输出列簇下的k-v
			for (KeyValue keyValue :rs.raw()) {
				System.out.println("列簇"+new String(keyValue.getFamily())+"===列:"+new String(keyValue.getQualifier())+"===值"+new String(keyValue.getValue()));
			}
		}
		scanner.close();//释放资源
		hTable.close();
	}
	
	public static  void deleteData(String tablename) throws IOException{
		HTable hTable = new HTable(conf, tablename);
		//设置要删除的列簇
		Delete delete = new Delete(getBytes("djt")); 
		//指定删除的列  或者列簇
		//delete.deleteColumn(getBytes("info"), getBytes("age"));
		delete.deleteFamily(getBytes("info"));
		hTable.delete(delete);
		
		hTable.close();
	}
	
	
	private static byte[] getBytes(String str) {
		// TODO Auto-generated method stub
		if(str == null){
			str = "";
		}
		return Bytes.toBytes(str);
	}
	

}
