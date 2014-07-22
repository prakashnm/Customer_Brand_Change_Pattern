package com.hadoop.brandswitch;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class FacebookBrandSwitchCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map(LongWritable inputKey, Text inputVal,Context context) throws IOException, InterruptedException
	{
	String[] splits=inputVal.toString().split("\t");
	context.write(new Text(splits[1]),new IntWritable(1));
	//context.write(new Text(splits[1],new IntWritable(1));
	}
	
}
