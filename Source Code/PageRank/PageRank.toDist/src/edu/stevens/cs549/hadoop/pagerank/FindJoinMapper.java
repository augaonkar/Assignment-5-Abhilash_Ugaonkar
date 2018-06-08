package edu.stevens.cs549.hadoop.pagerank;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindJoinMapper extends Mapper<LongWritable, Text, Text, Text>  {

	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * Join final output with link name
		 * input: key: nodeId+rank, text: adjacent list
		 * input: key: nodeId, text: name
		 * 
		 * output: key: nodeId, text: rank
		 * ouput: key: nodeId, text:names
		 */
		
		String[] section;
		if (line.contains(":")) {
			int ind = line.indexOf(":");
			section = new String[2];
			section[0] = line.substring(0, ind);
			section[1] = line.substring(ind + 1, line.length());
		} else {
			section = line.split("\t"); // Splits it into two parts. Part 1: node+rank | Part 2: adj list
		}
		

		if (section.length > 2) // Verfiy Correct Data Format
		{
			throw new IOException("INVALID DATA FORMAT");
		}
		String[] noderank = section[0].split("\\+");
		if(noderank.length == 1) {
			// nodeID along with name
			context.write(new Text(noderank[0]), new Text(PageRankDriver.MARKER_NAME + section[1].trim()));
		}
		
		if(noderank.length == 2) {
			// nodeID along with  rank
			context.write(new Text(noderank[0]), new Text(PageRankDriver.MARKER_RANK + noderank[1]));
		}
	}

	
}
