package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	public final double DAMPING_FACTOR = 0.85;

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) 
     throws IOException, InterruptedException {
       double pr= 0;
       double old_pr= 0;
       String receivingNodes = "";
       for (Text val : values) {
    	   StringTokenizer tokenizer = new StringTokenizer(val.toString());
    	   System.out.println(val.toString());
    	   
    	   if (!tokenizer.hasMoreTokens())
    		   return;
    		   
    	   String tok= tokenizer.nextToken();
    	   if (tok.equals("pr")){
    		   String received_pr= tokenizer.nextToken();
    		   System.out.println("Received pr value: " + received_pr);
    		   pr+= Double.valueOf(received_pr);
    	   } else {
    		   //skip over node number; get page rank
    		   tok= tokenizer.nextToken();
    		   old_pr= Double.valueOf(tok);
    		   System.out.println("Old pr= " + old_pr);
    		   while(tokenizer.hasMoreTokens()){
	    		   receivingNodes+= " " + tokenizer.nextToken();
    		   }
    	   }
       }
       System.out.println("incoming pr sum: " + pr);
       org.apache.hadoop.mapreduce.Counter residualSum= context.getCounter(SimplePageRank.CounterGroup.RESIDUAL_SUM);
       pr = (double) (DAMPING_FACTOR * pr + (1-DAMPING_FACTOR) / (double) SimplePageRank.NUMBER_OF_NODES );
       System.out.println("New pr: " + pr);
       double residual= Math.abs(old_pr - pr) / pr;
       residualSum.setValue((long) ((long) residualSum.getValue() + residual * 100000));
       System.out.println("key: " + key);
       Text value= new Text(pr + receivingNodes);
       System.out.println("value: " + value);
       context.write(key, value);
   }
}