package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FindJoinReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {

		/*
		 * Join final output with linked name
		 * input: key: nodeId, val: (mark_rank)rank
		 * input: key: nodeId, val: (mark_name))name
		 * 
		 * output: key: nodeId+names, text: rank
		 */
		Iterator<Text> iter = values.iterator();
		String nodeName = "";
		String rank  = "";
		while(iter.hasNext()) {
			String temp = iter.next().toString();
			if(temp.startsWith(PageRankDriver.MARKER_NAME)) {
				nodeName = temp.replaceAll(PageRankDriver.MARKER_NAME, "");
			} 
			if(temp.startsWith(PageRankDriver.MARKER_RANK)) {
				rank = temp.replaceAll(PageRankDriver.MARKER_RANK, "");
			} 
		}
		
		context.write(new Text(key + "+" + nodeName) , new Text(rank));
		
	}

}
