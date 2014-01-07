package com.poc.loganalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LogAnalysisDriver  {

	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Log Analysis");
		job.setJarByClass(LogAnalysisDriver.class);
		job.setMapperClass(LogAnalysisMapper.class);
	    //job.setReducerClass(MaxTemperatureReducer.class);
		//job.setCombinerClass(TokenCounterCombiner.class);
	    
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
		FileInputFormat.addInputPath(job, new Path("/user/hduser/LogAnalysis"));
	    FileOutputFormat.setOutputPath(job, new Path("/user/hduser/LogAnalysisOutput"));
		job.waitForCompletion(true);
		
	}

}
