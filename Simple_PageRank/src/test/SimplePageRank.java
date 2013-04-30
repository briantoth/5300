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
	public final static long NUMBER_OF_NODES = 685229;
	public static enum CounterGroup {TOTAL_NODES, RESIDUAL_SUM};
	
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
			FileOutputFormat.setOutputPath(job, new Path("s3n://bdt25-5300-mr/output" + (i+1) +"/"));
			    
			job.waitForCompletion(true);
//			long totalNodes= job.getCounters().findCounter(SimplePageRank.CounterGroup.TOTAL_NODES).getValue();
//			System.out.println("totalNodes: " + totalNodes);
			long totalNodes=  NUMBER_OF_NODES;
			float residualSum= job.getCounters().findCounter(SimplePageRank.CounterGroup.RESIDUAL_SUM).getValue();
			System.out.println("residualRandom: " + residualSum);
			float average_residual= residualSum / totalNodes;
			//undo decimal point removal
			average_residual /= 100000;
			System.out.println("Average residual for run " + i + " is: " + average_residual);
		}
	 }
        
}