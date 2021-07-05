package com.anz.validator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RecordCountValidator {

	public boolean isRecordCountSameAsTag(Dataset<Row> dataRDD, long tagRecordCount) {
		
		return (dataRDD.count() == tagRecordCount);
	}
	
	

}
