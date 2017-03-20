
package BigData;
import io.netty.handler.codec.http.HttpHeaders.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.sound.sampled.Line;

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

import com.codahale.metrics.Counter;

public class TopTenBusiness_Q3 {
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text(); // type of output key
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("::");
			word.set(line[2]);
			one.set(Double.parseDouble(line[3]));
			context.write(word, one);
		}	
	}
	
	
	public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
			double sum = 0; // initialize the sum for each keyword
			int count = 0;
			
			for(DoubleWritable itemCount:values){
				sum+=itemCount.get();
				count++;
			}
			result.set(sum/count);
			context.write(key, result);
		}
	}
	
	public static class Map1 extends Mapper<LongWritable, Text, IntWritable, Text>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//send everything to one reducer
			context.write(one, value);
		}
		
	}
	
	public static class Reduce1 extends Reducer<IntWritable,Text,Text,DoubleWritable> {
		
		private static HashMap<String, Double> RatingMp = new HashMap();
        private Text businessId = new Text();
        private static DoubleWritable avg = new DoubleWritable();
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			 for (Text value : values) {
	            	String[] line = value.toString().split("\\s+");
	                String businessId = line[0];
	                Double avgRating = Double.parseDouble(line[1]);
	                RatingMp.put(businessId, avgRating);
	            }
			 TreeMap<String, Double> descSortedMap = new TreeMap(new ValueComparator(RatingMp));
			 descSortedMap.putAll(RatingMp);
	         int count = 0;
	         
	         for (Entry<String, Double> entry : descSortedMap.entrySet()) {
	                count++;
	                businessId.set(entry.getKey());
	                avg.set(entry.getValue());
	                context.write(businessId, avg);
	                if (count == 10) {
	                    break;
	                }
	            }
		}
	}
	static class ValueComparator implements Comparator {

        HashMap map;

        public ValueComparator(HashMap map) {
            this.map = map;
        }

        public int compare(Object keyA, Object keyB) {

            Double valueA = (Double) map.get(keyA);
            Double valueB = (Double) map.get(keyB);

            if (valueA == valueB || valueB > valueA) {
                return 1;
            } else {
                return -1;
            }

        }
    }
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text bId = new Text();
		private Text otherBusinessData = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("::");
			String businessId = line[0].trim();
			String addr = line[1].trim();
			String categories = line[2].trim();
			
			//table name is appended with this Emit 
			String emitString = "business" + "::" + addr + "::" + categories;
			bId.set(businessId);
			otherBusinessData.set(emitString);
			context.write(bId, otherBusinessData);
		}
	}
	
	public static class Map3
	extends Mapper<Object, Text, Text, Text>{

		private Text bId = new Text();
		private Text otherBusinessData = new Text();
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String line = value.toString();

			String[] outputQ2 = line.split("\\s+");

			String businessId = outputQ2[0].trim();
			//table name is appended with this Emit
			String emitString = "rating" + "::" + outputQ2[1].trim();

			bId.set(businessId);
			otherBusinessData.set(emitString);
			context.write(bId ,otherBusinessData);
		}

	}
	

	public static class Reduce2 extends Reducer<Text,Text,Text,Text> {
		private Text neededBusinessData = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			String bussinessid = key.toString();
			String addr = "";
			String cat  = "";
			String rating = "";
			int count = 0;
			Boolean business_flag = false;
			Boolean rating_flag = false;
			for(Text value: values)
			{
				String[] line = value.toString().split("::");
				if(line[0].equals("business") && business_flag == false )
				{
					business_flag = true;
					addr = line[1];
					cat = line[2];
					count++;
				}
				if(line[0].equals("rating") && rating_flag == false)
				{
					rating_flag = true;
					rating = line[1];
					count++;
				}
			}
			if(count == 2)
			{
				String output = addr + "  " + cat + "  " + rating;
				neededBusinessData.set(output);
				context.write(key, neededBusinessData);
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length < 4) {
			System.out.println("invalid arguments");
			System.exit(2);
		}
		// create a job with name "ratings"
		Job job = new Job(conf, "TopTenBusiness_Q3_1");
		job.setJarByClass(TopTenBusiness_Q3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean mr = job.waitForCompletion(true);
	
	if(mr)
	{
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"TopTenBusiness_Q3_2");
		job1.setJarByClass(TopTenBusiness_Q3.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
		// set output key type
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(DoubleWritable.class);
		//set the HDFS path of the input data
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		//Wait till job completion
		
		boolean mr1 = job1.waitForCompletion(true);
		
		if(mr1)
		{
			Job job2 = Job.getInstance(conf, "TopTenBusiness_Q3_3");

			/*business - input from business file*/
			MultipleInputs.addInputPath(job2, new Path(otherArgs[3]), TextInputFormat.class, Map2.class);

			/*rating - input from reducer1 output*/
			MultipleInputs.addInputPath(job2,new Path(otherArgs[2]), TextInputFormat.class, Map3.class);
			
			job2.setReducerClass(Reduce2.class);

			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
		
	}
	
	}
}

