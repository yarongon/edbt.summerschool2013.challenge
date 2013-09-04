import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Attributes
 *  1. age
 *  2. workclass
 *  3. fnlwgt
 *  4. education
 *  5. education-num
 *  6. marital-status
 *  7. occupation
 *  8. relationship
 *  9. race
 * 10. sex
 * 11. capital-gain
 * 12. capital-loss
 * 13. hours-per-week
 * 14. native-country
 * 
 * The rules: 
 *   1. education --> educationnumber
 *   2. occupation | country --> hours
 *   3. if hours <= 20 --> salary <= 50 or if hours >= 60 ---> >50
 *   4. occupation | country | hours --> salary
*/
public class DataCleanMR {

	public static class FDMapper extends Mapper<Object, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] valueArray = value.toString().split(",");
			
			// Rule 1
			outKey.set("1," + valueArray[3]); // rule id, value of left-hand-site
			outValue.set(valueArray[15] + "," + valueArray[4]); // tuple id, tuple value			
			context.write(outKey, outValue);
			
			// Rule 2
			
		}
	}
	
	public static class MyPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numOfReducers) {
			return (key.charAt(0) & Integer.MAX_VALUE) % numOfReducers;
		}
		
	}

	public static class FDReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean violationOccured = false;
			int ruleId = Integer.parseInt(key.toString().split(",")[0]);
			int reducerId = context.getTaskAttemptID().getTaskID().getId();
			Set<Text> violationTable = new HashSet<Text>();
			String previousAttrValue = null;
			
			for (Text value : values) {
				String attrValue = value.toString().split(",")[1]; 
				if (previousAttrValue == null) {
					previousAttrValue = attrValue;
				}
				String violation = String.format("%d;%d;%s;%d;%s;%s",
						reducerId,
						ruleId,
						"adult",
						Integer.parseInt(value.toString().split(",")[0]),
						"educationNum",
						attrValue
						);
				
				violationTable.add(new Text(violation));
				
				if (!attrValue.equals(previousAttrValue)) {
					violationOccured = true;
				}
				previousAttrValue = attrValue;
			}
			
			if (violationOccured) {
				for (Text violation : violationTable) {
					context.write(violation, null);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: lineitemq1 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(DataCleanMR.class);
		job.setMapperClass(FDMapper.class);
		//job.setPartitionerClass(MyPartitioner.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(FDReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
