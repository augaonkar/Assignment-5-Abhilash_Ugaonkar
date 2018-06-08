package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/* 
		 * TODO: Just echo the input, since it is already in adjacency list format.
		 */

		/*
		 * split the line by symbol ":", and output key, adjacent list to reducer
		 */
		String[] p = line.split(":");
		if(p != null && p.length == 2) {
			context.write(new Text(p[0].trim()), new Text(p[1]));
		}

	}

}
