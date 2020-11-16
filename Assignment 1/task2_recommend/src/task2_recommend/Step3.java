package task2_recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step3 {
	public static class Step31_UserVectorSplitterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		@Override
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			//processes score matrix so it will also include items not scored by user as 0.0
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			HashMap<String, Float> hash = new HashMap<String, Float>();
			int userId = Integer.parseInt(tokens[0]);
			StringTokenizer item;
			
			for(int i=1;i<tokens.length;i++) { //tokens format is itemID:score like the output of step1
				item = new StringTokenizer(tokens[i], ":"); //split itemID:score by ":"
				String itemId = item.nextToken();
				String score = item.nextToken();
				hash.put(itemId, Float.parseFloat(score)); //map scored items (with their score) into hash table
			}
			
			k.set(userId);
			
			for(int i=0;i<500;i++) { //loop through all 500 items
				if(hash.containsKey(Integer.toString(i))){ 
					v.set(i + ":" + hash.get(Integer.toString(i)));//if hash table has scored item with ID i then set value to itemID:score
				}else {
					v.set(i + ":" + "0.0");//if item with ID i is not scored by user (not found in hash table) then set value to itemID:0.0
				}
				context.write(k, v); //each user will list all 500 items, but previously not scored items have 0.0 score instead
			}
		}
	}
	public static void run1(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		//get configuration info
    	Configuration conf = Recommend.config();
    	//get I/O path
        Path input = new Path(path.get("Step3Input1"));
        Path output = new Path(path.get("Step3Output1"));
        //delete the last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //set job
        Job job =Job.getInstance(conf,"Step3_1");
		job.setJarByClass(Step3.class);
		
		job.setMapperClass(Step31_UserVectorSplitterMapper.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//run job
		job.waitForCompletion(true);
	}
	
	public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		@Override
		public void map(LongWritable key, Text values,Context context)
				throws IOException, InterruptedException {
				//since step 2 output is (itemID1:itemID2 value), this step changes the format to (itemID1 itemID2:value)
				String[] tokens = Recommend.DELIMITER.split(values.toString());
				StringTokenizer item;
				item = new StringTokenizer(tokens[0], ":");
				int itemRow = Integer.parseInt(item.nextToken());
				String itemCol = item.nextToken();
				k.set(itemRow);
				v.set(itemCol+ ":" + tokens[1]);
				context.write(k, v);
		}
	}

	public static void run2(Map<String, String> path) throws IOException,
		ClassNotFoundException, InterruptedException {
		//get configuration info
		Configuration conf = Recommend.config();
		//get I/O path
		Path input = new Path(path.get("Step3Input2"));
		Path output = new Path(path.get("Step3Output2"));
		// delete the last saved output 
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
		Job job =Job.getInstance(conf, "Step3_2");
		job.setJarByClass(Step3.class);

		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		// run job
		job.waitForCompletion(true);
}
}
