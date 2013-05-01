package blocked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    /**
     * 
     * ************** New BlockedPageRank Map Comment
     * Input is "dummy_value node_num node_block_num current_page_rank receiving_node1 
     * 			 receving_node1_block_num receiving_node2 receiving_node2_block_num ..."
     * 
     * Mapper must send the following information to the reducer (From assignment page):
     *  1) The set of vertices of its Block, with their current PageRank values 
     *     and their lists of outgoing edges
	 *	2) The set of edges entering the block from outside, with the current PageRank
	 *     value that flows along each entering edge.
	 *    
	 *  NOTE: The keys for everything outputted are block_numbers, NOT node_numbers
	 *  Outputted is
	 *  
	 * 
	 *  key: node_block_num
	 *  value:  node_info node_num pr receiving_node1 receiving_node1_block_num receiving_node2 receiving_node_block_num2...
	 *   --> This corresponds to (1) from above. We include the block numbers for each receiving node so that
	 *       we can recreate the initial input format when we emit from the reducer
	 *  
	 *  And then for each receiving_node where receiving_node_block_num != node_block_num
	 *  key: receiving_node_block_num
	 *  value: boundary_edge receiving_node pr/def(node_num)
	 *  	--> This corresponds to (2) from above. Namely these are the edges that go from 
	 *          one block to another. 
	 *          
     * ************** Original SimplePageRank Map comment
     * Input is "node_num current_page_rank receiving_node1 receiving_node2 receiving_node3 ..." 
     * 
     *
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        System.out.println(line);
        
        if (!tokenizer.hasMoreTokens())
        	return;
        
        //The first value is a dummy value since the output of
        //the reducer is 'key' 'actual value' and we only care about the actual value
        tokenizer.nextToken();
        
        String nodeNumString= tokenizer.nextToken();
        System.out.println("Node number is: " + nodeNumString);
        LongWritable nodeNum= new LongWritable(Long.parseLong(nodeNumString));
        
        String blockNumString = tokenizer.nextToken();
        System.out.println("Block number is: " + blockNumString);
        LongWritable blockNum= new LongWritable(Long.parseLong(blockNumString));
        
        if (!tokenizer.hasMoreTokens())
        	return;
        
        float pr= Float.parseFloat(tokenizer.nextToken());
        System.out.println("Page rank is: " + pr);
        
        ArrayList<LongWritable> receivingNodes= new ArrayList<LongWritable>();
        ArrayList<LongWritable> receivingBlockNums = new ArrayList<LongWritable>();
        while (tokenizer.hasMoreTokens()) {
        	receivingNodes.add(new LongWritable(Long.parseLong(tokenizer.nextToken())));
        	
        	if(! tokenizer.hasMoreTokens()) 
        		System.err.println("ERROR:: There was a node number without a block number.\nLine is: " + line);
        	else
        		receivingBlockNums.add(new LongWritable(Long.parseLong(tokenizer.nextToken())));
        }
        
        String allReceivingNodes= "";
        LongWritable n, b;
        
        //Here we go through and build a list of w1 w1_block_num w2 w2_block_num ...
        //to send to the reducer. We also check, and for each w in receivingNodes where
        //w_block_num != block_num we emit 
        //	key: w_block_num
   	   //	value: boundary_edge w pr/len(receivingNodes)
        for(int i = 0; i < receivingBlockNums.size(); i++) {
        	n = receivingNodes.get(i);
        	b = receivingBlockNums.get(i);
        	allReceivingNodes += n + " " + b + " ";
        	
        	if(! b.equals(blockNum)) {
        		//This is a boundary edge, so we need to emit some extra stuff
        		context.write(b, new Text("boundary_edge " +  n + " " + pr/allReceivingNodes.length()));
        	}
        }
        
        /** TODO DETERMINE IF THIS IS STILL NECCESSARY FOR BLOCKED PAGE RANK
         
        //This following line is neccessary for cases where a node only
        //has outbound links. If this wasn't here then if node A only had
        //outbound links then A would never be emitted as a key by the mapper
        //(since the keys emitted are the edge sinks). Therefore we would not
        //be able to calculate pagerank for it in the reducer. 
        context.write(nodeNum, new Text("" + 0));
        
        */
        
        context.write(blockNum, new Text("node_info " + nodeNum + " " + pr + " " + allReceivingNodes));
        
    }
 } 
