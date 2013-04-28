package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import test.Map.Counter;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	public final double DAMPING_FACTOR = 0.85;
	
	@Override
   public void reduce(LongWritable key, Iterable<Text> values, Context context) 
     throws IOException, InterruptedException {
       float pr= 0;
       String receivingNodes = "";
       for (Text val : values) {
    	   StringTokenizer tokenizer = new StringTokenizer(val.toString());
    	   System.out.println(val.toString());
    	   
    	   if (!tokenizer.hasMoreTokens())
    		   return;
    		   
    	   String nextToken= tokenizer.nextToken();
    	   if (nextToken.equals("pr")){
    		   pr+= Float.parseFloat(tokenizer.nextToken());
    	   } else {
    		   receivingNodes+= nextToken;
    		   while(tokenizer.hasMoreTokens()){
	    		   receivingNodes+= " " + tokenizer.nextToken();
    		   }
    	   }
       }
       pr = DAMPING_FACTOR * pr + (1-DAMPING_FACTOR) / context.getCounter(Counter.TOTAL_NODES).getValue();
       context.write(key, new Text(key + " " + pr + " " + receivingNodes));
   }
}