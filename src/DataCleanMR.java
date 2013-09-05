import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 * 10. sex varchar(255), 
 * 11. capitalgain int, 
 * 12. capitalloss int, 
 * 13. hoursperweek int, 
 * 14. nativecountry varchar(255), 
 * 15. agrossincome varchar(255)
 * 
 * The rules: 
 *   1. education --> educationnumber
 *   2. occupation | country --> hours
 *   3. if hours <= 20 --> salary <= 50 or if hours >= 60 ---> >50
 *   4. occupation | country | hours --> salary
*/
public class DataCleanMR {

	public static final String TABLE_NAME = "adult";
	
	public static class FDMapper extends Mapper<LongWritable, Text, IntStringPairWritable, LongStringPairWritable> {

		private IntStringPairWritable outKey    = new IntStringPairWritable();
		private LongStringPairWritable outValue = new LongStringPairWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] valueArray = value.toString().split(",");
			String hoursStr = valueArray[12];
			int hours = 0;
			try{
				hours = Integer.parseInt(hoursStr);
			}catch(Exception ex){
				// this means that this row contains attribute names.
				return;
			}
			long tupleId = key.get(); //Integer.parseInt(valueArray[15]);
			outValue.setLong(tupleId); // tuple id
			/*
			 * Rule 1:
			 * education --> educationnumber
			 * */
			outKey.setInt(1); // rule id
			outKey.setString(valueArray[3]); // value of left-hand-side
			
			outValue.setString(valueArray[4]); // tuple value (educationnumber)
			context.write(outKey, outValue);
			
			/*
			 * Rule 2:
			 * occupation | country --> hours
			 * */
			String occupationCountryKey = valueArray[6]+","+valueArray[13]; // occupation + country
			String salary = valueArray[14];
			outKey.setInt(2);
			outKey.setString(occupationCountryKey);

			outValue.setString(hoursStr);
			context.write(outKey, outValue);
			
			/*
			 * Rule 4
			 * occupation | country | hours --> salary
			 * */
			outKey.setInt(4);
			outKey.setString(occupationCountryKey + hours);

			outValue.setString(salary);
			context.write(outKey, outValue);
			
			/*
			 * Rule 3:
			 * if hours <= 20 --> salary <= 50 or if hours >= 60 ---> >50
			 * */
			if((hours<=20 && !salary.equals("<=50K")) || (hours>=60 && !salary.equals(">50K"))){
				outKey.setInt(3);
				outKey.setString(valueArray[12]); // hours

				outValue.setString(salary);
				context.write(outKey, outValue);
			}
		
			
		}
	}
	
	public static class MyPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numOfReducers) {
			return (key.charAt(0) & Integer.MAX_VALUE) % numOfReducers;
		}
		
	}
	

	public static class FDReducer extends Reducer<IntStringPairWritable, LongStringPairWritable, Text, Text> {
		Random randomGen = new Random(System.currentTimeMillis());
		
		public void reduce(IntStringPairWritable key, Iterable<LongStringPairWritable> values, Context context) throws IOException, InterruptedException {
			boolean violationOccured = false;
			int ruleId = key.getInt();			
			int violationId = context.getTaskAttemptID().getTaskID().getId() + randomGen.nextInt(Integer.MAX_VALUE);
			Set<Text> violationTable = new HashSet<Text>();
			String previousAttrValue = null;
			String violation;
			
			
			for (LongStringPairWritable value : values) {

				String attrValue = value.getString(); 
				if (previousAttrValue == null) {
					previousAttrValue = attrValue;
				}
				
				violation = generateVialotionTableRaw(ruleId, violationId, value.getLong(), attrValue);
				
				violationTable.add(new Text(violation));
				
				if (!violationOccured && !attrValue.equals(previousAttrValue)) {
					violationOccured = true;
				}
				//previousAttrValue = attrValue;
			}
			
			if (violationOccured) {
				for (Text v : violationTable) {
					context.write(v, null);
				}
			}
			violationTable = null;
		}
		/**
		 * 
		 * @param ruleId
		 * @param violationId
		 * @param tupleId
		 * @param attrValue
		 * @return
		 */
		private String generateVialotionTableRaw(int ruleId, int violationId, long tupleId, String attrValue){
			String attributeName = "";
			switch(ruleId){
				case 1:
					attributeName = "educationNum";
					break;
				case 2:
					attributeName = "hoursperweek";
					break;
				case 3:
				case 4:
					attributeName = "agrossincome";
					break;
				default:
					attributeName = "";
			}
			return String.format("%d;%d;%s;%d;%s;%s",
					violationId,
					ruleId,
					TABLE_NAME,
					tupleId,
					attributeName,
					attrValue
				);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: DataCleanMR <in> <out> <#red>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");

		job.setJarByClass(DataCleanMR.class);
		job.setMapperClass(FDMapper.class);
		//job.setPartitionerClass(MyPartitioner.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(FDReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setOutputKeyClass(IntStringPairWritable.class);
		job.setOutputValueClass(LongStringPairWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
