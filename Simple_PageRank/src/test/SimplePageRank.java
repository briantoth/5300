package test;
        
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class SimplePageRank {
	static enum CounterGroup {TOTAL_NODES, AVERAGE_RESIDUAL};
	
	public static void main(String[] args) throws Exception {
		
		for(int i= 0; i < 5; i++){
		
			Configuration conf = new Configuration();
			Job job = new Job(conf, "SimplePageRank");
			job.setJarByClass(SimplePageRank.class);
			
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			    
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			    
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			
			FileInputFormat.addInputPath(job, new Path("s3n://bdt25-5300-mr/output" + i + "/part-r-00000"));
			FileOutputFormat.setOutputPath(job, new Path("s3n://bdt25-5300-mr/output" + (i+1) + "/part-r-00000"));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    
			job.waitForCompletion(true);
			float average_residual= job.getCounters().findCounter(CounterGroup.AVERAGE_RESIDUAL).getValue()/job.getCounters().findCounter(CounterGroup.TOTAL_NODES).getValue();
			System.out.println("Average residual for run " + i + " is: " + average_residual);
		}
	 }
        
}