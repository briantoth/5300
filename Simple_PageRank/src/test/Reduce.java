package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import test.SimplePageRank.CounterGroup;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	public final double DAMPING_FACTOR = 0.85;
	
	@Override
   public void reduce(LongWritable key, Iterable<Text> values, Context context) 
     throws IOException, InterruptedException {
       float pr= 0;
       float old_pr= 0;
       String receivingNodes = "";
       for (Text val : values) {
    	   StringTokenizer tokenizer = new StringTokenizer(val.toString());
    	   System.out.println(val.toString());
    	   
    	   if (!tokenizer.hasMoreTokens())
    		   return;
    		   
    	   String nextToken= tokenizer.nextToken();
    	   if (nextToken.equals("pr")){
    		   String received_pr= tokenizer.nextToken();
    		   System.out.println("Received pr value: " + received_pr);
    		   pr+= Float.parseFloat(received_pr);
    	   } else {
    		   //skip over node number; get page rank
    		   nextToken= tokenizer.nextToken();
    		   old_pr= Float.parseFloat(nextToken);
    		   while(tokenizer.hasMoreTokens()){
	    		   receivingNodes+= " " + tokenizer.nextToken();
    		   }
    	   }
       }
       System.out.println("incoming pr sum: " + pr);
       System.out.println("Current counter value: " + context.getCounter(CounterGroup.TOTAL_NODES).getValue());
       pr = (float) (DAMPING_FACTOR * pr + (1-DAMPING_FACTOR) / (float) context.getCounter(CounterGroup.TOTAL_NODES).getValue());
       System.out.println("New pr: " + pr);
       float residual= Math.abs(old_pr - pr) / pr;
       context.getCounter(CounterGroup.AVERAGE_RESIDUAL).setValue((long) (context.getCounter(CounterGroup.AVERAGE_RESIDUAL).getValue() + residual * 100000));
       context.write(key, new Text(key + " " + pr + " " + receivingNodes));
   }
}