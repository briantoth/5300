package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
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
    		   pr+= Float.parseFloat(tokenizer.nextToken());
    	   } else {
    		   //skip over node number; get page rank
    		   nextToken= tokenizer.nextToken();
    		   old_pr= Float.parseFloat(nextToken);
    		   while(tokenizer.hasMoreTokens()){
	    		   receivingNodes+= " " + tokenizer.nextToken();
    		   }
    	   }
       }
       context.write(key, new Text(key + " " + pr + " " + receivingNodes));
   }
}