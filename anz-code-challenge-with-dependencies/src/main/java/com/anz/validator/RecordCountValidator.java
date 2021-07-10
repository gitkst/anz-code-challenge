package com.anz.validator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;

import com.anz.io.InputFileReaderANZ;
import com.anz.io.OutputWriterANZ;
import com.anz.util.FileConstants;
import com.anz.validator.ifc.ValidatorANZIfc;

public class RecordCountValidator implements ValidatorANZIfc {

	public boolean isRecordCountSameAsTag(Dataset<Row> dataRDD, long tagRecordCount) {
		
		return (dataRDD.count() == tagRecordCount);
	}

	
	private int performValidation(Dataset<Row> dataRDD, long tagCount) {
		
		System.out.println("recordCountCheck == Started");
	    boolean isRecordCountSameAsTag = isRecordCountSameAsTag(dataRDD, tagCount);
	    System.out.println("recordCountCheck == Started"+ isRecordCountSameAsTag);
		
    	if(! isRecordCountSameAsTag) {
    		
	    	OutputWriterANZ.writeExmptyFile(FileConstants.FILE_NAME_RECORD_COUNT_CHECK_INVALID);
	    	return 1;
    	}
    	
    	return 0;
    	
	}

	@Override
	public int validate( long tagCount, JSONObject schemaFileJSON, String data, String tagFileName, SparkSession spark) {
		
		// TODO Auto-generated method stub
		System.out.println(getClass().getName());
		
		Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark);
		
		return performValidation(dataRDD, tagCount);
	}
	
	

}
