package com.hadoop.test;

import java.io.IOException;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.test.Temperature.TemperatureMapper;
import com.hadoop.test.Temperature.TemperatureReducer;

/*����,
 * �Ƚϳ���,������ͬ�ٿ���ɵ���ĸ�Ƿ�һ��
 * ��ȡ��,תΪ�ַ���.
 * 
 * ��������һ����������ϵıȽ�.
 * map()����,
 * 
 * 
 */

public class GussWorld extends Configured implements Tool{
	public static class GussWorldMapper extends Mapper<LongWritable, Text, Text,Text>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			//��ȡÿ��ֵ
			String line = value.toString().trim();  		//��ȡÿ�е�ֵ  �������˿ո�ȥ��
			char [] ch = line.toCharArray(); //ÿ��תΪ����
			Arrays.sort(ch);						//���ַ�����˳������
			String s= String.valueOf(ch);			//�ַ�������תΪ�ַ���
			if(s!=null){
				context.write(new Text(s),value);
				}
	}
	}
		public static class GussWorldReducer extends Reducer<Text, Text, Text, Text>{
			private Text text = new Text();
			public void reduce(Text key,Iterable<Text> values ,Context context) throws IOException, InterruptedException {
				//ͳ����ͬkeyֵ�Ĳ�ͬ���� 
				StringBuilder sbr =new StringBuilder();
				for(Text text:values){
					if(text!=null){
					sbr.append(text);
					sbr.append("\t");
					}
				}
				
					context.write(key, new Text(sbr.toString()));
				} 	
					
			}
		

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] args0 = {"hdfs://wang:9000/anagram",
		"hdfs://wang:9000/anagram/out"};
//ִ��run����[��ȡ�����ļ�,����,��������·��]
int ec =ToolRunner.run(new Configuration(),new GussWorld(),args0 );
//���������ݷ���״̬�˳�
System.exit(ec);  

	}
	



	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		//��ȡ�����ļ�
		Configuration conf = new Configuration();
		Path path = new Path(arg0[1]);
		FileSystem hdfs = path.getFileSystem(conf);
		if(hdfs.isDirectory(path)){
			hdfs.delete(path,true);
		}
		// 设置主类
		Job job =new Job(conf,"line");
		job.setJarByClass(GussWorld.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		//���岽ָ��mapper��reducer
		job.setMapperClass(GussWorldMapper.class);
		job.setReducerClass(GussWorldReducer.class);
		//����map()he reduce()�������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//���߲�:�ύ��ҵ
		return job.waitForCompletion(true)?0:1;
	
		
	}}
