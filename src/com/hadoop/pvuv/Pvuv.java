package com.hadoop.pvuv;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.stringLiteralSequence_return;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**************离线统计puuv MR项目*******************/

/*
 * 通过sougou lab 提供的数据  统计网站访问量
 * 00:00:00	26706283607513126	[芭蕾舞剧《天鹅湖》]	5 6	you.video.sina.com.cn/b/5924814-1246200450.html
 *00:00:00	01532039495118448	[不锈钢]	1 1	www.51bxg.com/
 *00:00:01	0014362172758659586	[明星合成]	64 21	link.44box.com/
 *00:00:01	8958844460084823	[骆冰下载]	6 3	www.sxjzy.com/css/l.asp?id=560
 *00:00:01	14918066497166943	[欧洲冠军联赛决赛]	4 1	s.sohu.com/20080220/n2
 * 时间   访问者id  收索内容   网址    结果排行  点击顺序   网址
 */


	public class Pvuv extends Configured implements Tool{

		public static void main(String[] args) throws Exception {
			String [] args0 = {"hdfs://sla1:9000/hadoop/SogouQ.reduced","hdfs://sla1:9000/hadoop/SogouQ"};  
			int ec =ToolRunner.run(new Configuration(),new Pvuv(),args0 );
			System.exit(ec);
		}
		public static class PvuvMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
			//定义一个常量one  
			private final static IntWritable ONE = new IntWritable(1);
			public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
				//读取文件内内容   按行读取
				String line = value.toString();
				//将字符串按"\t" 分隔拆分成为数组
				String [] lineArray = line.split("\t");
				//将网址作为key 每次出现均为一个pv 同一个用户和同一个网站出现一次为一次uv
				String webAddr = lineArray[4]; 
				if(webAddr != null){
					
				context.write(new Text(webAddr), ONE);
				}

			}
		
		}

		public static class PvuvReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
			private IntWritable result = new IntWritable();
		
			public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
				//��һ��:ͳ����ͬ����վ��������ֵ
				int sum = 0;
				for(IntWritable val:values){
					sum +=val.get();
				}
				//�ڶ�����ͬһ������վ��ƽ������
				result.set(sum);
				context.write(key, result);
			}
		}

	
		
		public int run(String[] args)throws Exception{
			Configuration conf =new Configuration();
			Path mypath = new Path(args[1]);
			FileSystem hdfs =mypath.getFileSystem(conf);
			if(hdfs.isDirectory(mypath)){
				hdfs.delete(mypath,true);
			}
			Job job = new Job(conf,"temperature");
			job.setJarByClass(Pvuv.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(PvuvMapper.class);
			job.setReducerClass(PvuvReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			return job.waitForCompletion(true)?0:1;
		}
}
