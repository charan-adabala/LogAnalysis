package com.visualpath.loganalysis.mapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.eclipse.core.runtime.Path;
/**
 * AccessLogMapper: AccessLogMapper performing the Access.log process
 * @author Charan Adabala
 *
 */
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key,Text value,Context context)
			throws java.io.IOException, InterruptedException {
		org.apache.hadoop.fs.Path outputPath = FileOutputFormat.getOutputPath(context);
		if (outputPath != null) {
			FileSystem fileSys = outputPath.getFileSystem(context.getConfiguration());
		      if (fileSys.exists(outputPath)) {
		    	org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/user/hduser/LogAnalysisOutput/AccessLog");
		        fileSys.create(path).close();
		      }
		}
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
