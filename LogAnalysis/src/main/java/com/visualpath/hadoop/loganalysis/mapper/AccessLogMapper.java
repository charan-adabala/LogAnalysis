package com.visualpath.hadoop.loganalysis.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * AccessLogMapper: AccessLogMapper performing the ccess.log process
 * @author Charan Adabala
 *
 */
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key,Text value,Context context)
			throws java.io.IOException, InterruptedException {
		
		String log  = value.toString();
		String filterdLine = log.replaceAll("[\"\\[\\]]","");
		String[] splitValue = filterdLine.split(" ");
		String filteredLog = splitValue[0]+","+splitValue[3]+","+splitValue[4]+","+splitValue[5]+","+splitValue[6]+","+splitValue[7]+
							","+splitValue[8]+","+splitValue[9]+","+splitValue[10]+","+splitValue[11];
		if(filteredLog != null && filteredLog.length() > 0){
			context.write(new Text(""), new Text(filteredLog));
		}
	};

}
