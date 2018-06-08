package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */

		/*
		 * Since default rank is 1, so we need only output node+rank and adjacency list
		 */
		int defualt_rank = 1;
		Iterator<Text> val = values.iterator();
		while(val.hasNext()) {
			context.write(new Text(key + "+" + defualt_rank), val.next());
		}
	}
}
