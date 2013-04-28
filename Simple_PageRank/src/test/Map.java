package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import test.SimplePageRank.CounterGroup;

public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    /**
     * Input is "node_num current_page_rank receiving_node1 receiving_node2 receiving_node3 ..." 
     * 
     * Mapper outputs two different key-value pairs.  One is the list of all of the edges coming out of this node keyed
     * by the name of the node, and the other is the the current rank of this node keyed by the name of each node
     * which receives an edge from this node
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        System.out.println(line);
        
        if (!tokenizer.hasMoreTokens())
        	return;
        
        String tok= tokenizer.nextToken();
        System.out.println(tok);
        LongWritable nodeNum= new LongWritable(Long.parseLong(tok));
        
        if (!tokenizer.hasMoreTokens())
        	return;
        
        float pr= Float.parseFloat(tokenizer.nextToken());
        ArrayList<LongWritable> receivingNodes= new ArrayList<LongWritable>();
        
        while (tokenizer.hasMoreTokens()) {
        	receivingNodes.add(new LongWritable(Long.parseLong(tokenizer.nextToken())));
        }
        
        String allReceivingNodes= "";
        for(LongWritable n : receivingNodes){
        	//build up a list to send to the reducer.  
        	//TODO: not sure if this is correct
        	allReceivingNodes += n + " ";
        	//send out the weighted PR for this node
	        context.write(n, new Text("pr " + pr/allReceivingNodes.length()));
        }
        

        context.write(nodeNum, new Text(allReceivingNodes));
        context.getCounter(CounterGroup.TOTAL_NODES).increment(1);
        //A line corresponds to a unique node, so to keep track
        //of how many nodes we have total we can just 
        context.write(nodeNum, value);

        
    }
 } 
