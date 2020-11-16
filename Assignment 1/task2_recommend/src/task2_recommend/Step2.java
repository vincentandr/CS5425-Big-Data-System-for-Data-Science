package task2_recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import recommendpkg.Step1.Step1_ToItemPreMapper;
//import recommendpkg.Step1.Step1_ToUserVectorReducer;

public class Step2 {
	public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString()); //tokens[0] is userID and each of the following tokens are itemID:score
            StringTokenizer item1, item2;
            for(int i=1; i<tokens.length ; i++) { //loop through all itemID:score
            	item1 = new StringTokenizer(tokens[i], ":"); //split itemID:score i by ":"
            	String itemId1 = item1.nextToken();
            	for(int j=1; j<tokens.length;j++) { //loop from first itemID:score to last itemID:score to build cooccurrence matrix
	            	item2 = new StringTokenizer(tokens[j], ":"); //split itemID:score j by ":"
	            	String itemId2 = item2.nextToken();
	            	k.set(itemId1+":"+itemId2); //map itemID i with itemID j (essentially map itemID row and itemID col)
	            	context.write(k, v); // key is itemID1:itemID2 and value is 1
            	}
            }
        }
    }

    public static class Step2_UserVectorToConoccurrenceReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
    	private IntWritable result = new IntWritable();
        private HashMap<String, Float> hash = new HashMap<String, Float>();
    	
        @Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
        	
        	int sum = 0;
        	for(IntWritable value:values) { //sum all the no. of counts for each itemID1:itemID2 key 
        		sum += Integer.parseInt(value.toString()); //value is always 1
        	}
        	result.set(sum);
        	context.write(key, result); //key and frequency, e.g. itemID1:itemID2	23
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    	//get configuration info
    	Configuration conf = Recommend.config();
    	//get I/O path
        Path input = new Path(path.get("Step2Input"));
        Path output = new Path(path.get("Step2Output"));
        //delete last saved output
        HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //set job
        Job job =Job.getInstance(conf,"Step2");
		job.setJarByClass(Step2.class);
		
		job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
		job.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
		job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//run job
		job.waitForCompletion(true);
    }
}


