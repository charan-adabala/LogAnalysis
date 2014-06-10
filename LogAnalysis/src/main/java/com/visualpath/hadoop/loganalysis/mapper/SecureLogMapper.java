package com.visualpath.hadoop.loganalysis.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * SecureLogMapper: SecureLogMapper performing the Secure.log process
 * 
 * @author Charan Adabala
 * 
 */
public class SecureLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String log = value.toString();
		if (!(log.contains("pam_unix") || log.contains("sudo")
				|| log.contains("Server listening on") || log
					.contains("Received disconnect"))) {
			String time = log.substring(4, 24);
			DateFormat readFormat = new SimpleDateFormat("MMM dd yyyy hh:mm:ss");
			DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss");
			Date date;
			String timeStamp = null;
			try {
				date = readFormat.parse(time);
				timeStamp = dateFormat.format(date);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			String www = log.substring(25, 29);
			String details = log.substring(30);
			String detailsContent = null;
			String filterdLine = details.replaceAll("[\"\\[\\]\\:]", "");
			String[] splitValue = filterdLine.split(" ");
			String keyValue = null;
			try {
				if (splitValue.length == 12) {
					detailsContent = splitValue[0] + "," + splitValue[1] + " "
							+ splitValue[2] + " " + splitValue[3] + " "
							+ splitValue[4] + " " + splitValue[5] + " "
							+ splitValue[6] + "," + splitValue[8] + ","
							+ splitValue[10] + "," + splitValue[11];
					keyValue = splitValue[8]+splitValue[0];
				} else if (splitValue.length == 10) {
					detailsContent = splitValue[0] + "," + splitValue[1] + " "
							+ splitValue[2] + " " + splitValue[3] + " "
							+ splitValue[4] + "," + splitValue[6] + ","
							+ splitValue[8] + "," + splitValue[9];
					keyValue = splitValue[6]+splitValue[0];
				}
				String filteredLog = timeStamp + "," + www + ","
						+ detailsContent;
				if (filteredLog != null && filteredLog.length() > 0) {
					context.write(new Text(keyValue), new Text(filteredLog));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	};
}
