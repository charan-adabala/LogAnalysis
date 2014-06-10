package com.visualpath.hadoop.loganalysis.client;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.visualpath.hadoop.loganalysis.mapper.AccessLogMapper;
import com.visualpath.hadoop.loganalysis.mapper.LogProcessMapper;
import com.visualpath.hadoop.loganalysis.mapper.LogProcessReducer;
import com.visualpath.hadoop.loganalysis.mapper.SecureLogMapper;
import com.visualpath.hadoop.loganalysis.mapper.SplunkAccessLogMapper;
/**
 * ProcessLogsClient: client to run the LogAnalysis job
 * @author Murthy
 *
 */
public class ProcessLogsClient {

  public static void main(String[] args) throws Exception  {
	  System.out.println("Started............");
	  
	  String ouputPath = args[1];
	//Deleting existing path -- starts
	Path p = new Path(ouputPath);
	Configuration config = new Configuration();
	//config.set("mapred.textoutputformat.separator", ",");
	//config.set("mapreduce.output.key.field.separator", ",");
	config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
	FileSystem fs = FileSystem.get(URI.create(ouputPath), config);
	fs.delete(p); 
    // Deleting existing path -- ends
	
    Job job = new Job();
    job.setJarByClass(ProcessLogsClient.class);
    job.setJobName("Log Process");
    FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	
    job.setMapperClass(LogProcessMapper.class);
    job.setReducerClass(LogProcessReducer.class);
    //job.setMapperClass(AccessLogMapper.class);
    //job.setMapperClass(SecureLogMapper.class);
    //job.setMapperClass(SplunkAccessLogMapper.class);
    //job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.waitForCompletion(true);
    System.out.println("Completed............");
  }
}
