package BigData;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PaloAlto {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		//Key
				private Text word = new Text();
		//value
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("::");
			Boolean found;
			String location = "Palo Alto";
			found = line[1].contains(location);
			if(found)
			{
				String category_list = line[2].substring(5, line[2].length() - 1);
				String[] categories = category_list.toString().split(", ");
				for(String item:categories){
					word.set(item);
					context.write(word,one);
				}
			}
		}
		
	}
	
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			//For every <key, List<value>>, neglect the list, we just want key. 
			context.write(key, NullWritable.get());
		}
	}
	
	
	// Driver program
	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	// get all args
	if (otherArgs.length != 2) {
		System.out.println("invalid arguments");
	System.exit(2);
	}
	// create a job with name "PaloAlto"
	Job job = new Job(conf, "PaloAlto");
	job.setJarByClass(PaloAlto.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	
	// set output key type
	job.setOutputKeyClass(Text.class);
	// set output value type
	job.setOutputValueClass(IntWritable.class);
	
	//set the HDFS path of the input data
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	// set the HDFS path for the output
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	//Wait till job completion
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
