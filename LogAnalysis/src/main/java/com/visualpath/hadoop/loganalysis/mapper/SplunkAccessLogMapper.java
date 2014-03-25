package com.visualpath.hadoop.loganalysis.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * SplunkAccessLogMapper: SplunkAccessLogMapper performing the access_combined.log process
 * @author Charan Adabala
 *
 */
public class SplunkAccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key,Text value,Context context)
			throws java.io.IOException, InterruptedException {
		
		String log  = value.toString();
		String filterdLine = log.replaceAll("[\"\\[\\]]","");
		String[] splitValue = filterdLine.split(" ");
		String filteredLog = splitValue[0]+","+splitValue[3]+","+splitValue[4]+","+splitValue[5]+","+splitValue[6].split("/")[0]+","+splitValue[6].split("/")[1]+","+splitValue[7]+
				","+splitValue[8]+","+splitValue[9]+","+splitValue[10]+","+splitValue[19];
		if(filteredLog != null && filteredLog.length() > 0){
			context.write(new Text(""), new Text(filteredLog));
		}
	};

}
