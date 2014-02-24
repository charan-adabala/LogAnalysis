package com.visualpath.loganalysis.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * SecureLogMapper: SecureLogMapper performing the Secure.log process
 * @author Charan Adabala
 *
 */
public class SecureLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key,Text value,Context context)
			throws java.io.IOException, InterruptedException {
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
	};
}