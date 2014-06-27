package com.visualpath.hadoop.loganalysis.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * AccessLogMapper: AccessLogMapper performing the access.log process
 * 
 * @author Charan Adabala
 * 
 */
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		String log = value.toString();
		String filterdLine = log.replaceAll("[\"\\[\\]]", "");
		String[] splitValue = filterdLine.split(" ");
		String filteredLog = null;
		try {
			if (splitValue.length == 28) {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "\t" + splitValue[11].split("/")[0]
						+ "\t" + splitValue[19].split("/")[0] + "\t"
						+ splitValue[21].split("/")[0];
				
			}else if (splitValue.length == 25) {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "\t" + splitValue[11].split("/")[0]
						+ "\t" + splitValue[22].split("/")[0] + "\t"
						+ splitValue[23].split("/")[0];

			}else if (splitValue.length == 27) {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "," + splitValue[11].split("/")[0]
						+ "\t" + "\t" + "\t" + "\t";
			}else if (splitValue.length == 22) {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "," + splitValue[11].split("/")[0]
						+ "\t" + splitValue[19].split("/")[0] + "\t" + splitValue[20].split("/")[0];
			}else if (splitValue.length == 30) {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "\t" + splitValue[11].split("/")[0]
						+ "\t" + splitValue[22].split("/")[0] + "\t"
						+ splitValue[28].split("/")[0];
			}else if (splitValue.length == 34) {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "\t" + splitValue[11].split("/")[0];
			}
			else {
				filteredLog = splitValue[0] + "\t" + splitValue[3] + "\t"
						+ splitValue[4] + "\t" + splitValue[5] + "\t"
						+ splitValue[6] + "\t" + splitValue[7] + "\t"
						+ splitValue[8] + "\t" + splitValue[9] + "\t"
						+ splitValue[10] + "\t" + splitValue[11].split("/")[0]
						+ "\t" + splitValue[20].split("/")[0] + "\t"
						+ splitValue[21].split("/")[0];
			}

			if (filteredLog != null && filteredLog.length() > 0) {
				context.write(new Text(splitValue[0] + " " + splitValue[3]),
						new Text(filteredLog));
				// splitValue[0]+splitValue[3]
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	};

}
