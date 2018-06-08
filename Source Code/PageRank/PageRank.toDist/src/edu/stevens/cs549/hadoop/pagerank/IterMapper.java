package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node+rank | Part 2: adj list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		/* 
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */
		// split node+rank
		String[] node_rank = sections[0].split("\\+"); 
		String node = String.valueOf(node_rank[0]);
		double rank = Double.valueOf(node_rank[1]);
		String adj_list = sections[1].toString().trim(); 

		String[] adj_nodes = adj_list.split(" ");
		int N = adj_nodes.length; // outgoing links number
		// 1/n * rank
		double weightOfPage = (double)1/N * rank; // calculate current page weight if outgoing to other links
		for(String adj_node : adj_nodes) {
			context.write(new Text(adj_node), new Text(String.valueOf(weightOfPage)));
		}
		// at the same time, emit current node's adj_list list with marker "ADJ:"
		context.write(new Text(node), new Text(PageRankDriver.MARKER + sections[1]));

	}

}
