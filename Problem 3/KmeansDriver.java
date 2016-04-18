package Project3.Query3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader; 
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;


public class KmeansDriver {
	 
    private static int iterNum=1;
    private static final int Max_Iter = 300;
    private static double ThreshHold = 0.01;
    private static String out = "/part-r-00000";
    private static List<Float> history = new ArrayList<Float>();
    //private static String txt = "/user/hadoop/input/Centers.txt";
	
	private static double getDist(double[] p, List<Double> c){
		double res = 0.0;
		for (int i =0; i < c.size(); i++){
			res += Math.pow(p[i] - c.get(i), 2);
		}
		return Math.sqrt(res);
	}
	
	public static class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		private List<List<Double>> centers = new ArrayList<List<Double>>();
		private int k;
		private int d;
		private Text t = new Text();
		
		public void setup(Context context) throws IOException,InterruptedException{
			d = 0; k = 0;
	        Path[] caches=DistributedCache.getLocalCacheFiles(context.getConfiguration());  
	        if(caches==null||caches.length<=0) System.exit(1);
	        
	        BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));  
	        String line;  
	        //  get the file data  
	        while((line=br.readLine())!=null){  
	            centers.add(new ArrayList<Double>());  
	            String[] str=line.split(",");
	            d = (d == 0)? str.length : d;
	            for(int i=0;i<str.length;i++){  
	                centers.get(k).add(Double.parseDouble(str[i]));    
	            }
	            k++; 
	        }
	        try{
	        	br.close();
	        }catch (Exception e){
	        	e.printStackTrace();
	        }
		}
		
		
		public void map(LongWritable key, Text value, Context context 
				)throws IOException, InterruptedException{
			
			String[] recordFields = value.toString().split(",");
			double[] vals = new double[recordFields.length];
			for (int i =0; i < vals.length; i ++){
				vals[i] = Double.parseDouble(recordFields[i]);
			}
			double minDist = Double.MAX_VALUE;
			double curdist;
			int index = 0;
			for (int j =0; j < centers.size(); j ++){
				curdist = getDist(vals, centers.get(j));
				if (curdist < minDist){
					minDist = curdist;
					index = j;
				}
			}
			String res = "";
			for (int i = 0; i < vals.length; i ++){
				res = res + vals[i] + ",";
			}
			res = res +"1";
			t.set(res);
			context.write(new IntWritable(index), t);
		}
	}
	
	public static class KmeansCombiner extends Reducer<IntWritable,Text,IntWritable,Text> {
		private int d = 0;
		
		public void setup(Context context) throws IOException,InterruptedException{
	        
	        Path[] caches=DistributedCache.getLocalCacheFiles(context.getConfiguration());  
	        if(caches==null||caches.length<=0) System.exit(1);
	        
	        BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));  
	        String line;  
	        //  get the file data  
	        if ((line=br.readLine())!=null) {
	            String[] str=line.split(",");
	            d = str.length;
	        }
	        try{
	        	br.close();
	        }catch (Exception e){
	        	e.printStackTrace();
	        }
		}
		
		
	    public void reduce(IntWritable key,Iterable<Text> values,Context context)throws InterruptedException, IOException{   
	        double[] sum=new double[d];  
	        int sumCount=0; 
	        for(Text t:values){  
	            String[] datastr=t.toString().split(",");   
	            for(int i=0;i<d;i++){  
	                sum[i]+=Double.parseDouble(datastr[i]);  
	            }  
	            sumCount = sumCount + Integer.parseInt(datastr[d]);
	        }    
	        StringBuffer sb=new StringBuffer();  
	        for(int i=0;i<d;i++){  
	            sb.append(sum[i] + ",");
	        }
	        sb.append(sumCount+"");
	        context.write(key, new Text(sb.toString()));  
	    } 
		
	}
	
	public static class KmeansReducer extends Reducer<IntWritable,Text,NullWritable,Text> {  
		
		private List<List<Double>> centers = new ArrayList<List<Double>>();
		private int k;
		private int d;
		private float diff = 0.0f;
		
		public void setup(Context context) throws IOException,InterruptedException{
			d = 0; k = 0;
	        Path[] caches=DistributedCache.getLocalCacheFiles(context.getConfiguration());  
	        if(caches==null||caches.length<=0) System.exit(1);
	        
	        BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));  
	        String line;  
	        //  get the file data  
	        while((line=br.readLine())!=null){  
	            centers.add(new ArrayList<Double>());  
	            String[] str=line.split(",");
	            d = (d == 0)? str.length : d;
	            for(int i=0;i<str.length;i++){  
	                centers.get(k).add(Double.parseDouble(str[i]));    
	            }
	            k++; 
	        }
	        try{
	        	br.close();
	        }catch (Exception e){
	        	e.printStackTrace();
	        }
		}
		
	    public void reduce(IntWritable key,Iterable<Text> values,Context context)throws InterruptedException, IOException{
	        double[] sum=new double[d];  
	        int sumCount=0; 
	        for(Text t:values){  
	            String[] datastr=t.toString().split(",");   
	            for(int i=0;i<d;i++){  
	                sum[i]+=Double.parseDouble(datastr[i]);  
	            }
	            sumCount = sumCount + Integer.parseInt(datastr[d]);
	        }      
	        StringBuffer sb=new StringBuffer();  
	        List<Double> cur;
	        for(int i=0;i<d;i++){  
	        	sum[i] = sum[i] / sumCount;
	            sb.append(sum[i]);  
	            if (i < d - 1) sb.append(",");
	        }
	        cur = centers.get(key.get());
	        diff = diff + (float) (Math.pow(getDist(sum, cur), 2));
	        context.write(null, new Text(sb.toString()));
	    }
	    

	    public void cleanup(Context context){
		    context.getCounter("Result", "Result").increment((long) diff * 1000);
	    }
	}
	
    public static float doIteration(int iterNum, String[] args) throws IOException, ClassNotFoundException, InterruptedException{  
        boolean flag=false;
        Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
        Job job = new Job(conf, "kmeans job"+" "+iterNum);  
        job.setJarByClass(KmeansDriver.class);  
        job.setMapperClass(KmeansMapper.class);  
        job.setMapOutputKeyClass(IntWritable.class);  
        job.setMapOutputValueClass(Text.class);    
        job.setCombinerClass(KmeansCombiner.class);  
        job.setReducerClass(KmeansReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);  
        job.setOutputValueClass(Text.class);      
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));    
        flag=job.waitForCompletion(true);
        return (float) (job.getCounters().findCounter("Result", "Result").getValue() / 1000);  
    }
    
	public static void main(String[] args) throws Exception{
		 long start = System.currentTimeMillis();
		 Configuration conf = new Configuration(true);
		 generateCenters(conf, Integer.parseInt(args[3]), "/intermediate"+out);
		 FileSystem fs = FileSystem.get(conf);
		 boolean flag = true;
		 while (flag && iterNum <= KmeansDriver.Max_Iter){
			 String[] path = new String[3];
			 path[0] = args[0];
				if (iterNum % 2 == 1) {
					//if (iterNum == 1) path[2] = txt;
					path[2] = args[1] + out;
					path[1] = args[2];
					fs.delete(new Path(path[1]), true);
					} else {
					path[1] = args[1];
					path[2] = args[2] + out;
					fs.delete(new Path(path[1]), true);
					}
				float Diff = doIteration(iterNum, path);
				history.add(Diff);
				if (Diff < ThreshHold) flag = false;
		        iterNum++;
		 }
		 for (int i =0; i < history.size(); i ++){
			 if (i > 0) System.out.print(",");
			 System.out.print(history.get(i));
			 
		 }
	}
	
	private static void generateCenters(Configuration conf, int k, String name) throws IOException{
		FileSystem hdfs =FileSystem.get(conf);
		Path homeDir=hdfs.getHomeDirectory();
		Path newFilePath=new Path(homeDir.toString() + name);
		StringBuilder sb = new StringBuilder();
		Random rand = new Random();
		for (int i=0; i < k; i++){
			for (int j = 0; j < 2; j ++){
				sb.append(rand.nextFloat() * 10000);
				if (j < 1) sb.append(",");
			}
			sb.append("\n");
		}
		byte[] byt=sb.toString().getBytes();
		FSDataOutputStream fsOutStream = hdfs.create(newFilePath);

		fsOutStream.write(byt);

		fsOutStream.close();
	}

}
