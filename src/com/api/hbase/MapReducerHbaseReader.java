package com.api.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReducerHbaseReader {
	

	public class WordCountHbaseMapper extends TableMapper<Text, Text>{
		protected void map(ImmutableBytesWritable key,Result values,
				Context context) throws IOException, InterruptedException {
			StringBuffer sBuffer = new StringBuffer("");
			//获取列簇content 下面的所有的值
			
			for (java.util.Map.Entry< byte[], byte[]> value : values
					.getFamilyMap("content".getBytes()).entrySet()) {
				
				String str = new String(value.getValue());
				if (str != null){
					sBuffer.append(str);
					
				}
				context.write(new Text(key.get()), new Text(new String(sBuffer)));	
			}
			
		}
		
		
	}
	
	public class  wordCountHbaseReducer  extends  Reducer<Text, Text, Text, Text>{
		private Text result = new Text();
		public void reduce ( Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException{
			for (Text val: value){
				result.set(val);
				context.write(key,result);
				
			}
		}
	}
	
	
	public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String tablename = "wordcout1";  //表名
		Configuration conf = HBaseConfiguration.create();  //实例化configuration
		conf.set("hbase.zookeeper.quorum","sla1,sla2,sla3"); //hbase 服务地址
		conf.set("hbase.zookeeper.property", "2181");
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "import from hbase to hdfs");
		job.setJarByClass(MapReducerHbaseReader.class);
		
		job.setReducerClass(wordCountHbaseReducer.class);
		TableMapReduceUtil.initTableMapperJob(tablename, new Scan(), WordCountHbaseMapper.class,  Text.class, Text.class, job, false);
		
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://sla1:9000/dajiangtai/out"));  
	        System.exit(job.waitForCompletion(true) ? 0 : 1);  
	} 
}
