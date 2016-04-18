package Project3.Query2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {
	
	
	public static class RecordMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable Outkey = new IntWritable();
		private Text val = new Text();
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{
			
			String[] recordFields = value.toString().split(",");
			int Salary =  Integer.parseInt(recordFields[3]);
			Outkey.set(Salary);
			val.set(recordFields[4]);
			context.write(Outkey, val);
		}
	}
	
	public static class RecordReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			int male = 0; 
			int female = 0;
			for (Text t : values){
				if (t.toString().equals("Female")) female++;
				else male ++;
			}
			String res = "MALE :" + male +", FEMALE :" + female;
			context.write(key, new Text(res));
			
		}
	}
	public static void main(String[] args) throws Exception{
		 long start = System.currentTimeMillis();
		 Configuration conf = new Configuration(true);
		 Job job = new Job(conf, "Json Query");
		 job.setJarByClass(Driver.class);
		 job.setInputFormatClass(JsonInputFormat.class);
		 
		 job.setMapperClass(RecordMapper.class);
		 job.setMapOutputKeyClass(IntWritable.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setReducerClass(RecordReducer.class);
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.waitForCompletion(true);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 long end = System.currentTimeMillis();
		 
		 System.out.println((end-start)/1000.0);

	}

}
