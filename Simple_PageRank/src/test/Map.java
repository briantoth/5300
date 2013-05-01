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
        
        if (!tokenizer.hasMoreTokens())
        	return;
        
        //get our node number
        String tok= tokenizer.nextToken();
        LongWritable nodeNum= new LongWritable(Long.parseLong(tok));
        
        if (!tokenizer.hasMoreTokens())
        	return;
        
        //get pagerank
        Double pr= Double.valueOf(tokenizer.nextToken());
        ArrayList<LongWritable> receivingNodes= new ArrayList<LongWritable>();
        
        while (tokenizer.hasMoreTokens()) {
        	receivingNodes.add(new LongWritable(Long.parseLong(tokenizer.nextToken())));
        }
        
        for(LongWritable n : receivingNodes){
        	//send out the weighted PR for this node
	        context.write(n, new Text("pr " + pr/receivingNodes.size()));
        }
        
        //A line corresponds to a unique node, so to keep track 
        context.getCounter(CounterGroup.TOTAL_NODES).increment(1);
        
        //send out information about old pagerank and edges which we link to
        context.write(nodeNum, value);
    }
 } 
