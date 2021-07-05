package com.anz.validator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class PrimaryKeyValidator {
	
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
        	
        	dataRDD.select(primaryKeyCol).distinct().show();
        	
        	System.out.println(" dataRDDCount == "+ dataRDDCount + "distinctDataCount ==" + distinctDataCount);
        	if(dataRDDCount == distinctDataCount ) {
        		return true;
        	} else {
        		return false;
        	}
        	
        	
        }
		
		return false;
	}
	
	public int isPrimaryKeyUnique(JSONObject schemaFileJSON, Boolean bool) {
		
		JSONArray primaryKeys = (JSONArray) schemaFileJSON.get("primary_keys");
		System.out.println(primaryKeys.size());
		//If there are more than one columns in Primary Key the validation fails.
        if(primaryKeys.size() == 1) {
        	bool = Boolean.valueOf(true);
        	System.out.println(bool.booleanValue());
        	return 0;
        }
		
        bool = Boolean.valueOf(false);
    	return 2;
		
	}

}
