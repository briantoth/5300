package blocked;
        
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class BlockedPageRank {
	public static enum CounterGroup {TOTAL_RESIDUAL, TOTAL_ITERATIONS, NUMBER_BLOCKS}

	public static final long TOTAL_NODES = 684997;
	private static final float EPISILON = 0.001f;
	
	/**
	 * Changing it entails changing the loop to a while loop
	 * that checks whether the PageRank has converged as well
	 * as changing what is printed out to match the values we need
	 * for part 5
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Calendar cal = Calendar.getInstance();
    	SimpleDateFormat sdf = new SimpleDateFormat("HHmmss");
    	String date = sdf.format(cal.getTime());
    	
    	
    	for(int iteration = 0; iteration < 6; iteration++) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "BlockedPageRank");
			job.setJarByClass(BlockedPageRank.class);
			
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			    
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			    
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			
			String inputPath;
			if(iteration == 0)
				inputPath = "s3n://edu-cornell-cs-cs5300s13-wjk56-project2/out_ec.txt";
			else
				inputPath = "s3n://edu-cornell-cs-cs5300s13-wjk56-project2/output" + date + "_" + (iteration - 1) + "/part-r-00000";
				
			
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path("s3n://edu-cornell-cs-cs5300s13-wjk56-project2/output" + date + "_" + iteration +"/"));
			    
			job.waitForCompletion(true);
			
			//Now that the job is completed we can get some stuff
			long totalIterations = job.getCounters().findCounter(BlockedPageRank.CounterGroup.TOTAL_ITERATIONS).getValue();
			long numBlocks = job.getCounters().findCounter(BlockedPageRank.CounterGroup.NUMBER_BLOCKS).getValue();
			float totalResid = job.getCounters().findCounter(BlockedPageRank.CounterGroup.TOTAL_RESIDUAL).getValue() / 100000.0f;
			
			float averageIterations = (float) totalIterations / (float) numBlocks;
			float averageResid = (float) totalResid / (float) numBlocks;
			
			System.out.println("After Iteration " + iteration);
			System.out.println("\tTotal Iterations: " + totalIterations);
			System.out.println("\tNumber blocks: " + numBlocks);
			System.out.println("\tTotal Residual: " + totalResid);
			System.out.println("\tAverage Iterations: " + averageIterations);
			System.out.println("\tAverage Residual: "+ averageResid);
			
			if(averageResid < EPISILON) {
				System.out.println("Convergence has been reached after iteration " + iteration + " ....stopping.");
				break;
			}
    	}

	 }
        
}