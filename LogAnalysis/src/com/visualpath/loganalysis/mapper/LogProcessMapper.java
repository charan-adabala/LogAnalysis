package com.visualpath.loganalysis.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * LogProcessMapper: Filter the log file and perform the process
 * @author Charan Adabala
 *
 */

public class LogProcessMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	protected void map(LongWritable key,Text value,Context context)
			throws IOException, InterruptedException {
		 FileSplit fileSplit = (FileSplit)context.getInputSplit();
		 String filename = fileSplit.getPath().getName();
		 if(filename.equalsIgnoreCase("access.log")){ 
			//LogProcessMapper.setFilename("access.log");
			AccessLogMapper accessLogMapper = new AccessLogMapper();
			accessLogMapper.map(key, value, context);
		 }else if(filename.equalsIgnoreCase("secure.log")){
			// LogProcessMapper.setFilename("secure.log");
			 SecureLogMapper secureLogMapper = new SecureLogMapper();
			 secureLogMapper.map(key, value, context);
			}
	}

}
