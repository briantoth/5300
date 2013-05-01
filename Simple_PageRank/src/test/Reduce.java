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
    	   
    	   //handle the 'normal' type of input which tells us the PageRank of a node that points to us
    	   if (tok.equals("pr")){
    		   String received_pr= tokenizer.nextToken();
    		   System.out.println("Received pr value: " + received_pr);
    		   pr+= Double.valueOf(received_pr);
    	   // read out information about outgoing edges (necessary to produce output) and old page_rank
    	   } else {
    		   //skip over node number; get page rank
    		   tok= tokenizer.nextToken();
    		   old_pr= Double.valueOf(tok);
    		   while(tokenizer.hasMoreTokens()){
	    		   receivingNodes+= " " + tokenizer.nextToken();
    		   }
    	   }
       }
       org.apache.hadoop.mapreduce.Counter residualSum= context.getCounter(SimplePageRank.CounterGroup.RESIDUAL_SUM);
       
       //compute the new PageRank
       pr = (double) (DAMPING_FACTOR * pr + (1-DAMPING_FACTOR) / (double) SimplePageRank.NUMBER_OF_NODES );
       
       //compute the residual and add it to the other residuals from this iteration
       double residual= Math.abs(old_pr - pr) / pr;
       residualSum.setValue((long) ((long) residualSum.getValue() + residual * 100000));
       Text value= new Text(pr + receivingNodes);
       
       //output the result to be used by the next mapper
       context.write(key, value);
   }
}