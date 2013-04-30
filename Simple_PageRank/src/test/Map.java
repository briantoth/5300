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
        
//        if (!tokenizer.hasMoreTokens())
//        	return;
//        
//        //skip bonus token
//        tokenizer.nextToken();
        
        if (!tokenizer.hasMoreTokens())
        	return;
        
        Double pr= Double.valueOf(tokenizer.nextToken());
        ArrayList<LongWritable> receivingNodes= new ArrayList<LongWritable>();
        
        while (tokenizer.hasMoreTokens()) {
        	receivingNodes.add(new LongWritable(Long.parseLong(tokenizer.nextToken())));
        }
        
        for(LongWritable n : receivingNodes){
        	//send out the weighted PR for this node
	        context.write(n, new Text("pr " + pr/receivingNodes.size()));
        }
        
        context.getCounter(CounterGroup.TOTAL_NODES).increment(1);
        //A line corresponds to a unique node, so to keep track
        //of how many nodes we have total we can just 
        context.write(nodeNum, value);
        
        System.out.println("Current total node count: " + context.getCounter(SimplePageRank.CounterGroup.TOTAL_NODES).getValue());

        
    }
 } 
