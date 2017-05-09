package com.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
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



/*
 * 1��дmap()����
 * 2��дreduce()����
 * 3��дrun()ִ�з���,��������mapreduce��ҵ
 * 4��main���������г���
 */

public class Temperature extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//�����������·��
		String[] args0 = {"hdfs://wang:9000/weather/",
				"hdfs://wang:9000/weather/out"};
		//ִ��run����[��ȡ�����ļ�,����,��������·��]
		int ec =ToolRunner.run(new Configuration(),new Temperature(),args0 );
		//���������ݷ���״̬�˳�
		System.exit(ec);
	}
	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			//��һ��,������վ����תΪstring����
			String line = value.toString();
			//�ڶ���,��ȡ����ֵ
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			//���岽������Ч���� �ж�����ֵ�Ƿ�Ϊ��Ч����ֵ,
			if(temperature!=-9999){
			//������,��ȡ������
			//��ȡ�����Ƭ
			FileSplit filesplit = (FileSplit) context.getInputSplit();
			//Ȼ���ȡ����վ��� ��ȡ·����ȡ����,��ȡ
			String weatherStationId = filesplit.getPath().getName().substring(5, 10);
			//���Ĳ�,�������.д��վ�����Ƽ��¶�ֵ
			context.write(new Text(weatherStationId), new IntWritable(temperature));
			}

		}
	
	}

	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
	
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			//��һ��:ͳ����ͬ����վ��������ֵ
			int sum = 0;
			int count =0;
			//ѭ��ͳһ����վ����������ֵ
			for(IntWritable val:values){
				//�����������ۼ�
				sum +=val.get();
				//ͳ�Ƽ��ϴ�С
				count++;				
			}
			//�ڶ�����ͬһ������վ��ƽ������
			result.set(sum / count);
			context.write(key, result);
		}
	}

	
	
	public int run(String[] args)throws Exception{
		//��һ����ȡ�����ļ�
		Configuration conf =new Configuration();
		//�ڶ������·�����ھ�ɾ��
		Path mypath = new Path(args[1]);
		FileSystem hdfs =mypath.getFileSystem(conf);
		if(hdfs.isDirectory(mypath)){
			hdfs.delete(mypath,true);
		}
		//����������job����
		Job job = new Job(conf,"temperature");
		job.setJarByClass(Temperature.class);
		//���Ĳ�ָ����������·�������·��
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//���岽ָ��mapper��reducer
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		//����map()he reduce()�������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//���߲�:�ύ��ҵ
		return job.waitForCompletion(true)?0:1;
	}



}
