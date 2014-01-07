package com.poc.loganalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LogAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key,Text value,Context context)
			throws IOException, InterruptedException {
		 FileSplit fileSplit = (FileSplit)context.getInputSplit();
		 String filename = fileSplit.getPath().getName();
		 if(filename.equalsIgnoreCase("access.log")){ // file name is used for based on file name we have to change the log for processing the file
			String log  = value.toString();
			String filterdLine = log.replaceAll("[\"\\[\\]]","");
			String[] splitValue = filterdLine.split(" ");
			String filteredLog = splitValue[0]+","+splitValue[3]+","+splitValue[4]+","+splitValue[5]+","+splitValue[6]+","+splitValue[7]+
								","+splitValue[8]+","+splitValue[9]+","+splitValue[10]+","+splitValue[11];
			if(filteredLog != null && filteredLog.length() > 0){
				context.write(new Text(""), new Text(filteredLog));
			}
		 }else{
		 }
	};

}
