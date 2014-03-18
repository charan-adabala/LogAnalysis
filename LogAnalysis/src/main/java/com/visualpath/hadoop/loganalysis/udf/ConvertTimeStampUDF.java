package com.visualpath.hadoop.loganalysis.udf;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
		int mm = 1;
		switch(mon) {
		case "Jan" : mm= 1; break;
		case "Feb" : mm= 2; break;
		case "Mar" : mm= 3; break;
		case "Apr" : mm= 4; break;
		case "May" : mm= 5; break;
		case "Jun" : mm= 6; break;
		case "Jul" : mm= 7; break;
		case "Aug" : mm= 8; break;
		case "Sep" : mm= 9; break;
		case "Oct" : mm= 10; break;
		case "Nov" : mm= 11; break;
		case "Dec" : mm= 12; break;
		default  : mm= 1; 
		}
		return mm;
	}
	
}
