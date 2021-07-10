package com.anz.validator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.anz.io.InputFileReaderANZ;
import com.anz.io.OutputWriterANZ;
import com.anz.util.FileConstants;
import com.anz.validator.ifc.ValidatorANZIfc;


public class PrimaryKeyValidator implements ValidatorANZIfc {
	
	public boolean isPrimaryKeyUnique(JSONObject schemaFileJSON, Dataset<Row> dataRDD) {
		
		JSONArray primaryKeys = (JSONArray) schemaFileJSON.get("primary_keys");
		System.out.println(primaryKeys);
		System.out.println("first Element "+primaryKeys.get(0));
		System.out.println(primaryKeys.size());
		
		String primaryKeyCol = (String) primaryKeys.get(0);
		
		//If there are more than one columns in Primary Key the validation fails.
        if(primaryKeys.size() == 1) {
        	
        	long dataRDDCount = dataRDD.count();
        	long distinctDataCount = dataRDD.select(primaryKeyCol).distinct().count();
        	
        	System.out.println(" dataRDDCount == "+ dataRDDCount + "distinctDataCount ==" + distinctDataCount);
        	if(dataRDDCount == distinctDataCount ) {
        		return true;
        	} else {
        		return false;
        	}
        	
        	
        }
		
		return false;
	}
	
	
	private int performValidation(Dataset<Row> dataRDD, JSONObject schemaFileJSON) {
		
		System.out.println("primaryKeyCheck == Started");
		boolean isPrimaryKeyUnique = isPrimaryKeyUnique(schemaFileJSON, dataRDD);
		System.out.println("primaryKeyCheck == Started" + isPrimaryKeyUnique);
		
    	if(!isPrimaryKeyUnique) {
    		
	    	OutputWriterANZ.writeExmptyFile(FileConstants.FILE_NAME_PRIMARY_KEY_CHECK_INVALID);
	    	return 3;
    	}
    	
    	return 0;
    	
	}

	@Override
	public int validate( long tagCount, JSONObject schemaFileJSON, String data, String tagFileName,  SparkSession spark) {
		
		// TODO Auto-generated method stub
		System.out.println(getClass().getName());
		
		Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark);
		
		return performValidation(dataRDD, schemaFileJSON);
	}

}
