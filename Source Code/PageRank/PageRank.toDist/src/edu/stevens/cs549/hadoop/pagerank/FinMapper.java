package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */

		String[] section = line.split("\t"); // nodeId+nodeName | rank

		if (section.length > 2) //Verify correct data format
		{
			throw new IOException("INVALID DATA FORMAT");
		}
		if (section.length != 2) {
			return;
		}
		// Reverse shuffle redce logic
		context.write(new DoubleWritable(0 - Double.valueOf(section[1])), new Text(section[0]));
	
	}

}
