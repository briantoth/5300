package blocked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import blocked.BlockedPageRank.CounterGroup;

public class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	private static final float EPSILON = 0.001f;
	private static final int MAX_ITERATIONS = 10;
	public final double DAMPING_FACTOR = 0.85;
	
	@Override
	/**
	 * TODO Change this from SimplePageRank reduce
	 * 
	 * Input is in the form
	 * key: block_num
	 * values: [value] where value is either of the form:
	 * 		node_info node_num pr receiving_node1 receiving_node1_block_num receiving_node2 receiving_node_block_num2
	 * 		-- or --
	 * 		boundary_edge receiving_node pr/def(node_num)
	 *
	 * First need to parse the input to form the following in memory data structures:
	 * 
	 *  PR[v] = current PageRank value of Node v for v ∈ B
	 *	BE = { <u, v> | u ∈ B ∧ u → v } = the Edges from Nodes in Block B
	 *	BC = { <u, v, R> | u ∉ B ∧ v ∈ B ∧ u → v ∧ R = PR(u)/deg(u) } = the Boundary Conditions
     *
     *  PR will be a map of node_number -> page_rank (Integer -> Float). We can construct this map
     *  using the node_info inputs
     *  
     *  BE will be a map of v -> set(u) (Integer -> HashSet<Integer>). Intuitively a mapping
     *  from node v in block B to a set of nodes in block B that link to it. We can construct
     *  this map using the node_info inputs. Node that all v and all u are members of B
     *  
     *  BC will be a map of v -> [R] (Integer -> ArrayList<Float>). R = PR(u) / deg(u).
     *  Note that we don't actually need to store u anywhere. 
	 */
   public void reduce(LongWritable key, Iterable<Text> values, Context context) 
     throws IOException, InterruptedException {
		Map<Integer, Float> PR = new HashMap<Integer, Float>();
		Map<Integer, HashSet<Integer>> BE = new HashMap<Integer, HashSet<Integer>>();
		Map<Integer, ArrayList<Float>> BC = new HashMap<Integer, ArrayList<Float>>();
		Map<Integer, Integer> blockMap = new HashMap<Integer, Integer>();
		Map<Integer, HashSet<Integer>> outLinks = new HashMap<Integer, HashSet<Integer>>();
		Map<Integer, Integer> deg = new HashMap<Integer, Integer>();
		int totalNodes = 600000; //TODO Fix this
		
		int blockNum = Integer.parseInt(key.toString());
		System.out.println("Block number is: " + blockNum);
		
		for(Text value : values) {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			System.out.println("\tValue: " + value.toString());
			
			if(! tokenizer.hasMoreTokens())
				return;
			
			String valueType = tokenizer.nextToken();
			System.out.println("\tValue type: " + valueType);
			if(valueType.equals("node_info")) {
				//Remainder of the input is of the form:
				//node_num pagerank w1 w1_block_num w2 w2 block_num ...
				int nodeNumber = Integer.parseInt(tokenizer.nextToken());
				float pageRank = Float.parseFloat(tokenizer.nextToken());
				int degree = 0;
				HashSet<Integer> sinkNodes = new HashSet<Integer>();
				PR.put(nodeNumber, pageRank);
				
				while(tokenizer.hasMoreTokens()) {
					int sinkNodeNumber = Integer.parseInt(tokenizer.nextToken());
					if(! tokenizer.hasMoreTokens()) {
						System.err.println("ERROR::Reducer has sinkNodeNumber without block number\nLine is " + value.toString());
					} else {
						degree+= 1;
						int sinkBlockNumber = Integer.parseInt(tokenizer.nextToken());
						
						blockMap.put(sinkNodeNumber, sinkBlockNumber);
						sinkNodes.add(sinkNodeNumber);
						if(sinkBlockNumber == blockNum) {
							if(! BE.containsKey(sinkNodeNumber)) {
								BE.put(sinkNodeNumber, new HashSet<Integer>());
							}
							BE.get(sinkNodeNumber).add(nodeNumber);
						}
					}
					
				}
				
				outLinks.put(nodeNumber, sinkNodes);
				deg.put(nodeNumber, degree);
			}else if(valueType.equals("boundary_edge")) {
				//Rest of input is of the form
				//receiving_node_num R
				int receivingNodeNum = Integer.parseInt(tokenizer.nextToken());
				float boundaryEdgePR = Float.parseFloat(tokenizer.nextToken());
				
				if(! BC.containsKey(receivingNodeNum))
					BC.put(receivingNodeNum, new ArrayList<Float>());
				
				BC.get(receivingNodeNum).add(boundaryEdgePR);
			} else {
				System.err.println("ERROR::Invalid value type for line : " + value.toString() + "\nValue type was: " + valueType);
			}
		}
		
		//Now that we built these stupid data structures we need
		//to iterate over the nodes in this block recalculating their
		//page rank until the blocks converge
		
		Map<Integer, Float> NPR, initialPR;
		
		float averageResid;
		int numIterations = 0;
		initialPR = PR;
		do {
			NPR = iterateBlockOnce(PR, BE, BC, deg, totalNodes);
			float totalResid = 0.0f;
			numIterations+= 1;
			
			//Calculate residuals for this iteration
			for(int v : PR.keySet()) 
				totalResid+= Math.abs(PR.get(v) - NPR.get(v));
			
			averageResid = totalResid / PR.size();
			PR = NPR;
		} while(averageResid > EPSILON && numIterations < MAX_ITERATIONS);
		
		float finalTotalResid = 0.0f;
		for(int v : initialPR.keySet())
			finalTotalResid+= Math.abs(PR.get(v) - initialPR.get(v));
		float finalResid = finalTotalResid / initialPR.size();
		//Now what does the reducer need to emit?
		//Mapper takes input:
		//node_num node_block_num current_page_rank receiving_node1 receving_node1_block_num receiving_node2 receiving_node2_block_num ..
		
		for(int v : PR.keySet()) {
			String line = "" + v + " " + key.toString() + " " + PR.get(v) + " ";
			for(int sink : outLinks.get(v)) {
				line+= sink + " " + blockMap.get(sink) + " ";
			}
			
			//TODO Figure out how to output correct form
			//context.write(key, value)
		}
		
   }
	
	/** Iterates over a block once and returns the new PR. Essentially a verbatim implentation
	 *  of the pseudo code given in the project description */
	private Map<Integer, Float> iterateBlockOnce(Map<Integer, Float> PR, Map<Integer, HashSet<Integer>> BE,
								 Map<Integer, ArrayList<Float>> BC, Map<Integer, Integer> deg, long totalNodes) {
		
		Map<Integer, Float> NPR = new HashMap<Integer, Float>();
		Set<Integer> V = PR.keySet();
		
		for(int v : V)
			NPR.put(v, 0.0f);
		
		for(int v : V) {
			for(int u : BE.get(v)) 
				NPR.put(v, NPR.get(v) + PR.get(u) / deg.get(u));
			
			for(float R : BC.get(v))
				NPR.put(v, NPR.get(v) + R);
			
			float npr = NPR.get(v);
			npr = (float) (DAMPING_FACTOR * npr + (1-npr) / totalNodes);
			NPR.put(v,  npr);
		}
		
		return NPR;
	
	}

}