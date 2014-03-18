package com.visualpath.hadoop.loganalysis.client;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.visualpath.hadoop.loganalysis.mapper.AccessLogMapper;
import com.visualpath.hadoop.loganalysis.mapper.LogProcessMapper;
import com.visualpath.hadoop.loganalysis.mapper.MyTextOutputFormat;
/**
 * ProcessLogsClient: client to run the LogAnalysis job
 * @author Murthy
 *
 */
public class ProcessLogsClient {

  public static void main(String[] args) throws Exception  {
	  System.out.println("Started............");
	  
	  String ouputPath = args[1];
	//String ouputPath = "/user/hduser/LogAnalysisOutput";
	
	//Deleting existing path -- starts
	Path p = new Path(ouputPath);
	Configuration config = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(ouputPath), config);
	fs.delete(p); 
    // Deleting existing path -- ends
	
    Job job = new Job();
    job.setJarByClass(ProcessLogsClient.class);
    job.setJobName("Log Process");
    FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	
    job.setMapperClass(LogProcessMapper.class);
    job.setMapperClass(AccessLogMapper.class);
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(MyTextOutputFormat.class);
    job.waitForCompletion(true);
    System.out.println("Completed............");
  }
}
