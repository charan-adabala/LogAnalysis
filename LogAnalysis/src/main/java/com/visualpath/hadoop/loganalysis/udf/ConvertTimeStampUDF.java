package com.visualpath.hadoop.loganalysis.udf;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDF to convert TimeSamp into Hive Timestamp format
 * @author Murthy
 *
 */
public final class ConvertTimeStampUDF extends UDF {
	public Text evaluate(final Text s) {
		System.out.println("ConvertTimeStampUDF: evaluate()");
		System.out.println("Input s = "+s);
		if (s == null) { 
			return null; 
		}
		String time=s.toString();
		DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		String timeStr = time.substring(0, 2)+"-"+convertMonth(time.substring(3, 6))+"-"+time.substring(7, 11)+" "+time.substring(12);
		System.out.println("timeStr: "+timeStr);
		java.util.Date date=null;
		try {
			date = (java.util.Date)formatter.parse(timeStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Timestamp ts = new Timestamp(date.getTime());
		System.out.println("Output ts = "+ts.toString());
		return new Text(ts.toString());
	}
	
	static int convertMonth(String mon) {
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(new SimpleDateFormat("MMM").parse(mon));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int monthInt = cal.get(Calendar.MONTH);
		return ++monthInt;
	}
	
}
