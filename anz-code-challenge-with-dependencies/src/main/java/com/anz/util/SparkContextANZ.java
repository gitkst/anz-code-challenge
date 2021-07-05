package com.anz.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkContextANZ {

	private SparkSession spark = null;
	
	public SparkSession sparkSession() {
		
		if(spark == null) {
			spark = SparkSession.builder().appName("Driver").config("spark.master", "local").getOrCreate();
		}
		
		return spark;
		
	}
		
}
