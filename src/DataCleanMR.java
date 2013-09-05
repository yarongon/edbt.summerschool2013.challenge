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
 *  1. age int,
 *  2. workclass varchar(255),
 *  3. fnlwgt int,
 *  4. education varchar(255),
 *  5. educationnum int,
 *  6. martialstatus varchar(255),
 *  7. occupation varchar(255), 
 *  8. relationship varchar(255),
 *  9. race varchar(255),
 *  10. sex varchar(255), 
 *  11. capitalgain int, 
 *  12. capitalloss int, 
 *  13. hoursperweek int, 
 *  14. nativecountry varchar(255), 
 *  15. agrossincome varchar(255)
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
			String[] splittedValues;
			String violation;
			for (Text value : values) {
				splittedValues = value.toString().split(",");
				String attrValue = splittedValues[1]; 
				if (previousAttrValue == null) {
					previousAttrValue = attrValue;
				}
				violation = String.format("%d;%d;%s;%d;%s;%s",
						reducerId,
						ruleId,
						"adult",
						Integer.parseInt(splittedValues[0]),
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
				for (Text v : violationTable) {
					context.write(v, null);
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
