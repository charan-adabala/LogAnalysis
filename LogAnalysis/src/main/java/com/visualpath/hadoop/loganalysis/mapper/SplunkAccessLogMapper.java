package com.visualpath.hadoop.loganalysis.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * SplunkAccessLogMapper: SplunkAccessLogMapper performing the
 * access_combined.log process
 * 
 * @author Charan Adabala
 * 
 */
public class SplunkAccessLogMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		String log = value.toString();
		String filterdLine = log.replaceAll("[\"\\[\\]]", "");
		String[] splitValue = filterdLine.split(" ");
		String filteredLog = null;
		try {
			if (!splitValue[5].contains("signon_error")) {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6].split("/")[0] + "\t"
						+ splitValue[6].split("/")[1] + "\t" + splitValue[7]
						+ "\t" + splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "\t" + splitValue[19];
			} else {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "," + " " + ","
						+ " " + "\t" + splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[18];
			}
			if (filteredLog != null && filteredLog.length() > 0) {
				context.write(new Text(splitValue[0]+" "+splitValue[3]), new Text(filteredLog));
				//splitValue[0]+splitValue[3]
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	};

}
