package com.anz.validator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class FieldValidator {
	
	public Dataset<Row> checkRecoundCount(Dataset<Row> dataRDD) {	
		System.out.println("checkRecoundCount");
		
		
		return setDirtyFlag(dataRDD);
		
	}
	public Dataset<Row> setDirtyFlag(Dataset<Row> dataRDD) {
		
		
		Dataset<Row> newdataRDD = dataRDD.withColumn("dirty_flag",functions.lit(0));
		
		return newdataRDD;
		
		
	}

}
