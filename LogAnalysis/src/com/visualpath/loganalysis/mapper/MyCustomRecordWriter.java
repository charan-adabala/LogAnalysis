package com.visualpath.loganalysis.mapper;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyCustomRecordWriter extends RecordWriter<Text, List<IntWritable>> {
    private DataOutputStream out;

    public MyCustomRecordWriter(DataOutputStream stream) {
        out = stream;
        try {
            out.writeBytes("results:\r\n");
        }
        catch (Exception ex) {
        }  
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
        //close our file
        out.close();
    }

    @Override
    public void write(Text arg0, List arg1) throws IOException, InterruptedException {
        //write out our key
        out.writeBytes(arg0.toString() + ": ");
        //loop through all values associated with our key and write them with commas between
        for (int i=0; i<arg1.size(); i++) {
            if (i>0)
                out.writeBytes(",");
            out.writeBytes(String.valueOf(arg1.get(i)));
        }
        out.writeBytes("\r\n");  
    }
}