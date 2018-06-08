package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		

		Iterator<Text> iter = values.iterator();
		double curr_Rank = 0; // default rank is 1 - d
		String adj_list = "";
		while(iter.hasNext()) {
			String line = iter.next().toString();
			if(!line.startsWith(PageRankDriver.MARKER)) {
				curr_Rank += Double.valueOf(line);
			} else {
				adj_list = line.replaceAll(PageRankDriver.MARKER, "");
			}
		}
		//sum calculations
		curr_Rank = 1 - d + curr_Rank * d;
		context.write(new Text(key + "+" + curr_Rank), new Text(adj_list));
	}
	
	
}
