package com.api.hbase;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


//MR api 读流程
public class MapReducerHbaseDriver {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String tablename = "wordcount1"; //hbase表名
		Configuration conf = HBaseConfiguration.create();//实例化
		
		//设置zookeeper
		conf.set("hbase.zookeeper.quorum", "sla1,sla2,sla3");
		//配置端口号
		conf.set("hbase.zookeeper.property.clientport", "2181");
		//配置master  这里配置的是客户端的接口   端口号新版为16010  旧版为60000  
		//新版本可以不用配置 hbasemaster
		//conf.set("hbasemaster", "sla0:16010");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(tablename)){
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
		}
		
		HTableDescriptor htd = new HTableDescriptor(tablename);
		HColumnDescriptor hcd = new HColumnDescriptor("content");
		htd.addFamily(hcd);  //创建列簇
		admin.createTable(htd);	//创建表 
		
		//创建任务
		
		Job job = new Job(conf, "import from hdfs to hbase");
		job.setJarByClass(MapReducerHbaseDriver.class);
		
		job.setMapperClass(WordCountMapperHbase.class);
		//job.setReducerClass(WordCountReducerHbase.class);
		//设置插入hbase时的相关操作
		TableMapReduceUtil.initTableReducerJob(tablename, WordCountReducerHbase.class, job, null, null, null, null, false);
		//设置mapper 的输出类型
        job.setMapOutputKeyClass(ImmutableBytesWritable.class)	;
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce输出类型
        
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        
        
        
		FileInputFormat.addInputPaths(job, "hdfs://sla1:9000/dajiangtai/someword.txt");
		System.exit(job.waitForCompletion(true)?0:1);	
	}
	
public static class WordCountMapperHbase extends org.apache.hadoop.mapreduce.Mapper<Object, Text,ImmutableBytesWritable,IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map (Object key, Text value,Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()){
			word.set(itr.nextToken());
			//输出到hbase的key类型为ImmutableByteswritable
			context.write(new ImmutableBytesWritable(Bytes.toBytes(word.toString())),one);
		}
		}
	}


	public static class WordCountReducerHbase  extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>{
		private IntWritable result = new IntWritable();
		public void reduce (ImmutableBytesWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable val:values){
				sum = sum +val.get();
				
			}
			Put put = new Put(key.get()); //put  实例化  key 代表主键  每个单词存一行
			//三个参数分别为  列簇 content ,列修饰符为count ,列值为词频
			put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
			context.write(key, put);  
			
		}
	}

}
