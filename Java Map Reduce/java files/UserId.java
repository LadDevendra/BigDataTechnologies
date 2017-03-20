package BigData;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class UserId {
	
	public static class Map extends Mapper<Object, Text, Text, Text>{
		private static HashMap<String, String> businesshashmp = new HashMap<>();
		private static BufferedReader br;
		private static Text output = new Text();
		private static Text userId = new Text();
		
		
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
			
			super.setup(context);

			@SuppressWarnings("deprecation")
			Path[] cachefiles = context.getLocalCacheFiles();
			
			
			if (cachefiles!=null)
			{
				
				for (Path ph:cachefiles) {
					if(true){
						
						businesshashmanpload(ph,context);
					}
				}
			}
		}
		
		private void businesshashmanpload(Path ph, Mapper<Object, Text, Text, Text>.Context context) {
			// TODO Auto-generated method stub

			String line = "";
			String value = "";

			try {
				
				br = new BufferedReader(new FileReader(ph.getName().toString()));
			
				while((line = br.readLine()) != null )
				{
					String[] line_add = line.split("::");
					if(line_add[1].contains("Stanford"))
						
						businesshashmp.put(line_add[0].trim(),value);
				}
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
				

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
			finally {
				if(br!=null)
					try {
						br.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}

		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {


			String[] ratingCSV = value.toString().trim().split("::");

			if(businesshashmp.containsKey(ratingCSV[2].trim()))
			{
				
				userId.set(ratingCSV[1].trim());
				output.set(ratingCSV[3]);
				context.write(userId,output);
			}
		}
		
		
	}
public static class Reduce extends Reducer<Text, Text, Text, Text> {

        
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text review : values){
            context.write(key, review); 
            }
        }
	}

	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		if (otherArgs.length < 2) {
			System.err.println("<HDFS input path for business.csv> <input path for review.csv> <output path>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "userId");
		DistributedCache.addCacheFile(new Path(args[0]).toUri(),job.getConfiguration());
		job.setJarByClass(UserId.class);
		job.setMapperClass(Map.class);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
