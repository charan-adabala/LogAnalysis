package com.visualpath.hadoop.loganalysis.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * LogProcessReducer: Filter the log file and perform the process
 * @author Charan Adabala
 *
 */

public class LogProcessReducer extends Reducer<Text, Text, Text, Text>{
	
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		 context.write(key, new Text(values.toString()));
	}
}
