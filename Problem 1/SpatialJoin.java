package Project3.Query1;

import java.io.IOException;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class SpatialJoin {
	static final int subw = 20;
	static final int subh = 5;
	
	private static List<int[]> RectGenerator(int topx, int topy, int height, int width){
		List<int[]> result = new ArrayList<int[]>();
		//Checking the validity
		if (topx - width < 0) width = topx;
		if (topy - height < 0) height = topy;
		int bottomx, bottomy,w,h;
		int leftx = topx - width;
		int lefty = topy - height;
		int rightx = topx % subw;
		int righty = topy % subh;
		topx = topx - topx % subw;
		topy = topy - topy % subh;
		for (int i = 0; i <= width / subw; i++){
			if (i == width / subw){
				bottomx = leftx;
				w = topx - (i - 1) * subw - leftx;
				}else{
					bottomx = topx - i * subw; 
					if (i == 0) w = rightx;
					else w = subw;
			}
			for (int j = 0; j <= height / subh; j ++){
				if (j == height / subh) {
					bottomy = lefty;
					h = topy - (j - 1) * subw - lefty;
				}else{
					bottomy = topy - j * subh;
					if (j == 0) h = righty;
					else h = subh;
				}
				result.add(new int[]{bottomx, bottomy, w, h});
			}
		}
		return result;
	}
	
	private static boolean iswithin(int x, int y, int[] range){
		if (x >= range[0] && x <= range[0] + range[2] && y >= range[1] && y <= range[1] + range[3]) return true;
		else return false;
	}
	public static class JoinGroupingComparator extends WritableComparator {
		public JoinGroupingComparator() {
			super (GridKey.class, true);
		}
		@Override
		public int compare (WritableComparable a, WritableComparable b){
			GridKey first = (GridKey) a;
			GridKey second = (GridKey) b;
			return first.getGridKey().compareTo(second.getGridKey());
		}
	}
	//public static class JoinSortingComparator extends WritableComparator {
	//	public JoinSortingComparator(){
	//		super(GridKey.class, true);
	//	}
	//	@Override
	//	public int compare (WritableComparable a, WritableComparable b){
	//		GridKey first = (GridKey) a;
	//		GridKey second = (GridKey) b;
	//		return first.compareTo(second);
	//	}
	//}
	
	public static class RectMapper extends Mapper<Object, Text, GridKey, Text>{
			
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int[] UserRange = new int[4];
	        UserRange[0] = conf.getInt("Userbottomx", -1);
	        UserRange[1] = conf.getInt("Userbottomy", -1); 
	        UserRange[2] = conf.getInt("Usertopx", Integer.MAX_VALUE);
	        UserRange[3] = conf.getInt("Usertopy", Integer.MAX_VALUE); 
			String[] tokens = value.toString().split(",");
			String ID = tokens[0];
			int topx = Integer.parseInt(tokens[1]);
			int topy = Integer.parseInt(tokens[2]);
			int width = Integer.parseInt(tokens[3]);
			int height = Integer.parseInt(tokens[4]);
			if (topx <= UserRange[2] && topx - width >= UserRange[0] && topy <= UserRange[3] && topy - height >= UserRange[1]){
				List<int[]> Rects = RectGenerator(topx, topy, width, height);
				for (int[] r : Rects){
					String tmp = ID;
					assert r.length == 4;
					for (int i =0; i < r.length; i++){
						tmp = tmp + "," + r[i];
					}
					String k = r[0] / subw + "," + r[1] / subh;
					GridKey recordKey = new GridKey(k, GridKey.RECT_RECORD.get());
					context.write(recordKey, new Text(tmp));
				}
			}
		}
	}
	public static class PointMapper extends Mapper<Object, Text, GridKey, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int[] UserRange = new int[4];
			List<Integer> Cx = new ArrayList<Integer>();
			List<Integer> Cy = new ArrayList<Integer>();
	        UserRange[0] = conf.getInt("Userbottomx", -1);
	        UserRange[1] = conf.getInt("Userbottomy", -1); 
	        UserRange[2] = conf.getInt("Usertopx", Integer.MAX_VALUE);
	        UserRange[3] = conf.getInt("Usertopy", Integer.MAX_VALUE); 
			String[] tokens = value.toString().split(",");
			int x = Integer.parseInt(tokens[0]);
			int y = Integer.parseInt(tokens[1]);
			if (x >= UserRange[0] && x <= UserRange[2] && y >= UserRange[1] && x <= UserRange[3]){
				if (x % subw == 0) Cx.add(x / subw - 1);
				Cx.add(x / subw);
				if (y % subh == 0) Cy.add(y / subh - 1);
				Cy.add(y / subh);
				for (Integer i : Cx){
					for (Integer j : Cy){
						String k = i + "," + j;
						GridKey recordKey = new GridKey(k, GridKey.POINT_RECORD.get());
						context.write(recordKey, new Text(x+","+y));
					}
				}
			}
		}
	}
	
	public static class JoinRecuder extends Reducer<GridKey, Text, NullWritable, Text>{
		public void reduce(GridKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String, int[]> map = new HashMap<String, int[]>();
			for (Text t : values){
				String[] vals = t.toString().split(",");
				if (key.getFlag().equals(GridKey.RECT_RECORD)){
					int[] tmp = new int[4];
					tmp[0] = Integer.parseInt(vals[1]);
					tmp[1] = Integer.parseInt(vals[2]);
					tmp[2] = Integer.parseInt(vals[3]);
					tmp[3] = Integer.parseInt(vals[4]);
					map.put(vals[0], tmp);
				}else{
					for (Map.Entry<String, int[]> e : map.entrySet()){
						int x = Integer.parseInt(vals[0]);
						int y = Integer.parseInt(vals[1]);
						if (iswithin(x, y, e.getValue())) context.write(NullWritable.get(), new Text(e.getKey() +",(" + vals[0] +"," + vals[1] +")"));
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Scanner s = new Scanner(System.in);
		Configuration conf = new Configuration();
		conf.set("Userbottomx", s.nextInt()+"");
		conf.set("Userbottomy", s.nextInt()+"");
		conf.set("Usertopx", s.nextInt()+"");
		conf.set("Usertopy", s.nextInt()+"");
		long start = System.currentTimeMillis();
		Job job = new Job(conf, "query 1");
		job.setJarByClass(SpatialJoin.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(GridKey.class);
		job.setMapOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RectMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PointMapper.class);
		job.setReducerClass(JoinRecuder.class);
		//job.setSortComparatorClass(JoinSortingComparator.class);
		job.setGroupingComparatorClass(JoinGroupingComparator.class);
		job.setNumReduceTasks(100);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    job.waitForCompletion(true);
	    long end = System.currentTimeMillis();
	    System.out.println((end-start)/1000.0);
	}
	
}

