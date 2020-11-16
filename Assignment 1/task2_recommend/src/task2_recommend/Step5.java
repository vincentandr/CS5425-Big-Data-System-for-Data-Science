package task2_recommend;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//

public class Step5 {
	public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		@Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		//you can use provided SortHashMap.java or design your own code.
        //ToDo
        //	
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			String itemId = tokens[0];
			for(int i=1;i<tokens.length;i++) {
				StringTokenizer user = new StringTokenizer(tokens[i], ":");
				String userId = user.nextToken();
				float val = Float.parseFloat(user.nextToken());
				if(userId.equals("221")) { //check if user id is same as 3 last number of my matric number
					k.set(userId);
					v.set(itemId + ":" + val);
					context.write(k, v); //outputs 221   itemId:val
				}
			}
		}
		
	}
	
	public static class Step5_FilterSortReducer extends Reducer<Text, Text, Text, Text> {
		private HashMap<String, Float> hm = new HashMap<String,Float>();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	StringBuilder sb = new StringBuilder();
        	for(Text value:values) {
        		StringTokenizer item = new StringTokenizer(value.toString(), ":");
        		String itemId = item.nextToken();
        		float val = Float.parseFloat(item.nextToken());
        		hm.put(itemId, val); //put itemId:val into hashmap for sorting
        	}
        	List<Entry<String, Float>> list = new LinkedList<Entry<String, Float>>();
        	list = SortHashMap.sortHashMap(hm); //sort itemId:val
        	for(Entry<String,Float> ilist : list){
    			sb.append(", " + ilist.getKey() + ":" + ilist.getValue());
    		}
        	Text v = new Text();
        	v.set(sb.toString().replaceFirst(", ", ""));
        	context.write(key, v); //key is still 221, value is itemId1:val,itemId2:val, ...
        }
    }
	
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// I/O path
		Path input = new Path(path.get("Step5Input"));
		Path output = new Path(path.get("Step5Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5_FilterSortMapper.class);
        job.setReducerClass(Step5_FilterSortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
	}
}
