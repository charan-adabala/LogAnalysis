package com.visualpath.hadoop.loganalysis.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTextOutputFormat extends
		FileOutputFormat<Text, List<IntWritable>> {
	@Override
	public org.apache.hadoop.mapreduce.RecordWriter<Text, List<IntWritable>> getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		// get the current path
		Path path = FileOutputFormat.getOutputPath(arg0);
		Path fullPath = null;
		// create the full path with the output directory plus our filename
		//if(inpuFileName.equalsIgnoreCase("access.log")){
			fullPath = new Path(path, "accessLog.txt");
		//}else if(inpuFileName.equalsIgnoreCase("secure.log")){
		//	fullPath = new Path(path, "secureLog.txt");
		//}
		// create the file in the file system
		FileSystem fs = path.getFileSystem(arg0.getConfiguration());
		FSDataOutputStream fileOut = fs.create(fullPath, arg0);

		// create our record writer with the new file
		return new MyCustomRecordWriter(fileOut);
	}
}