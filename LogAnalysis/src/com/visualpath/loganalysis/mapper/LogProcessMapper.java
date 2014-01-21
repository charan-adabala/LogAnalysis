package com.visualpath.loganalysis.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LogProcessMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key,Text value,Context context)
			throws IOException, InterruptedException {
		 FileSplit fileSplit = (FileSplit)context.getInputSplit();
		 String filename = fileSplit.getPath().getName();
		 //String filteredLog = null;
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
			 	String log = value.toString() ;
			if (!(log.contains("pam_unix") || log.contains("sudo")
					|| log.contains("Server listening on") || log.contains("Received disconnect"))) {
				String timeStamp = log.substring(0, 24);
				String www = log.substring(24, 29);
				String details = log.substring(30);
				String detailsContent  = null;
				String filterdLine = details.replaceAll("[\"\\[\\]\\:]", "");
					String[] splitValue = filterdLine.split(" ");
					if(splitValue.length == 12){
						detailsContent = splitValue[0]+","+splitValue[1]+" "+splitValue[2]+" "+splitValue[3]+" "+splitValue[4]+" "+splitValue[5]+
								" "+splitValue[6]+","+splitValue[8]+","+splitValue[10]+","+splitValue[11];
					}else if(splitValue.length == 10){
						detailsContent = splitValue[0]+","+splitValue[1]+" "+splitValue[2]+" "+splitValue[3]+" "+splitValue[4]+","+splitValue[6]+","+splitValue[8]+","+splitValue[9];
					}
					String filteredLog = timeStamp+","+www+","+detailsContent;
					if(filteredLog != null && filteredLog.length() > 0){
						context.write(new Text(""), new Text(filteredLog));
					}
				}
			 	 
		 }
		
	};

}
