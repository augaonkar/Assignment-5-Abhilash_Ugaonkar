package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.util.Iterator;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/* 
		 * TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */

		Iterator<Text> iter = values.iterator();
		double diff = 0; 
		// Rank 1 Calculation
		if(iter.hasNext()) {
			ranks[0] = Double.valueOf(iter.next().toString());
		}
		// Rank 2 Calculation
		if(iter.hasNext()) {
			ranks[1] = Double.valueOf(iter.next().toString());
		}
		// Difference Calculation
		diff = Math.abs(ranks[0] - ranks[1]);
		System.out.println( key.toString() + "  " + diff);
		context.write(key, new Text(String.valueOf(diff)));
	}
}
