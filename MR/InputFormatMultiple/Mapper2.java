package com.programs.MR.InputFormatMultiple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
	{
		String[] line=value.toString().split(",");		
		context.write(new Text(line[0]), new IntWritable(Integer.valueOf(line[1])));
	}
}
