package com.anz.validator.ifc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;

public interface ValidatorANZIfc {
	
	public int validate( long tagCount, JSONObject schemaFileJSON, String dataFileName, String tagFileName,  SparkSession spark);

}
