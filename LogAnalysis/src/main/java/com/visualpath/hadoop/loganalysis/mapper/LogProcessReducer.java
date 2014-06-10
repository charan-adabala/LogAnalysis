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
	
	protected void reduce(Text key, Text values,
			Context context)
			throws IOException, InterruptedException {
			System.out.println(values);
			String log =  values.toString().replace("\t", "");
		 context.write(key, new Text(log));
	}
}
