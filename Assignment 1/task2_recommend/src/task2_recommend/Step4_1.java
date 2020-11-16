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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//

public class Step4_1 {
	public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        // you can solve the co-occurrence Matrix/left matrix and score matrix/right matrix separately
		private String flag;
		private int i=-1, j=0, x=-1, y=0;
        private String prev = "";
        private Text k = new Text();
        private Text v = new Text();
        private static HashMap<String, Float> hashm1 = new HashMap<String, Float>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// data set
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            Text k = new Text();
            Text v = new Text();
            
            //step3_1 is score matrix, step3_2 is cooccurrence matrix
            if(flag.equals("step3_1")) { //assuming score matrix is comprised of (j,i)
            	String userId = tokens[0];
            	if(!prev.equals(userId)) {
            		i++; //new column
        			j=0; //reset row to 0
            	}
            	prev = userId;
				StringTokenizer item = new StringTokenizer(tokens[1], ":");
				String itemId = item.nextToken();
				Float score = Float.parseFloat(item.nextToken());
				hashm1.put(Integer.toString(i) + ":" + Integer.toString(j), score); //save value of i column and j row with dimensions of 1000:500
            	j++;
            }else if(flag.equals("step3_2")) { 
            	//assuming cooccurrence matrix is comprised of (x,y) where y item column is the same as j item row above
            	//since during matrix multiplication, left matrix is moving right while right matrix is moving down
            	
            	String itemId1 = tokens[0];
            	if(!prev.equals(itemId1)) { // each line may have the same userID, thus if userID changes that means it's a start of a new column i
            		x++; //new row
        			y=0; //reset column to 0
            	}
            	prev = itemId1;
            	StringTokenizer item = new StringTokenizer(tokens[1], ":");
            	String itemId2 = item.nextToken();
            	//ver 1 (either use ver 1 or ver 2, to avoid errors i use ver 1)
            	Float cooccurrence = Float.parseFloat(item.nextToken());
            	float result = 0.0f;
            	if(hashm1.containsKey(Integer.toString(x) + ":" + Integer.toString(y)))
            		result = cooccurrence * hashm1.get(Integer.toString(x) + ":" + Integer.toString(y)); //multiply value from cooccurrence table by value from score matrix
				k.set(Integer.toString(x) + Integer.toString(y)); //still wrong, don't know the perfect keys to use
				v.set(itemId1 + ":" + itemId2 + ":" + Float.toString(result));
		        context.write(k,v);
		        //ver 2, this one should have the correct logic but produces unknown error and runs for very long tiem due to massive loops
//            	for(int counter = 0;counter<1000;counter++) {
//            		float result = cooccurrence * hashm1.get(Integer.toString(counter) + ":" + Integer.toString(y));
//					k.set(Integer.toString(x) + ":" + Integer.toString(counter));
//					v.set(itemId1 + ":" + Integer.toString(counter+1) + ":" + Float.toString(result));
//			        context.write(k,v);
//            	}
            	y++;
            }
        }

    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
    	private final static Text k = new Text();
    	private final static Text v = new Text();
    	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //ToDo
           	String itemId1 = "";
        	String userId = "";
        	float result = 0.0f;
        	// from the grouped values by key, just do the sum since multiplication is done by mapper
        	for(Text value:values) {
        		StringTokenizer item = new StringTokenizer(value.toString(), ":");
        		itemId1 = item.nextToken();
        		userId = item.nextToken();
        		float val = Float.parseFloat(item.nextToken());
        		result += val;
        	}
        	k.set(itemId1);
        	v.set(userId + ":" + result); //outputs itemId1   userId:result
        	context.write(k, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	//get configuration info
		Configuration conf = Recommend.config();
		// get I/O path
		Path input1 = new Path(path.get("Step4_1Input1"));
		Path input2 = new Path(path.get("Step4_1Input2"));
		Path output = new Path(path.get("Step4_1Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
        Job job =Job.getInstance(conf);
        job.setJarByClass(Step4_1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_1.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_1.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}
