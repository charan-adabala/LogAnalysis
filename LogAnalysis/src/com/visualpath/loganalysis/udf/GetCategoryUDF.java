package com.visualpath.loganalysis.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDF to fetch category from URL
 * @author Murthy
 *
 */
public final class GetCategoryUDF extends UDF {
	public Text evaluate(final Text s) {
		return new Text(getCategory(s.toString()));
	}
	
	private  String getCategory(String url) {
		String category="";
		if(url.contains("categoryId")) {
			String[] split = url.split("categoryId");

			if(split[1].contains("&")) {
				int amp_loc = split[1].indexOf('&');
				category = split[1].substring(1, amp_loc);
				System.out.println("amp_loc: "+amp_loc);
				System.out.println(split[1].substring(1, amp_loc));
			} else {
				category = split[1].substring(1);
				System.out.println(split[1].substring(1));
			}
		}
		return category;
	}

	
}
