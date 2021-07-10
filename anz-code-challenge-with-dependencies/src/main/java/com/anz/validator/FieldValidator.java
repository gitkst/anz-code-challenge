package com.anz.validator;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

import com.anz.io.InputFileReaderANZ;
import com.anz.io.OutputWriterANZ;
import com.anz.util.CodeChallengeUtil;
import com.anz.util.FileConstants;
import com.anz.validator.ifc.ValidatorANZIfc;

public class FieldValidator implements ValidatorANZIfc {
	
	public Dataset<Row> checkRecoundCount(Dataset<Row> dataRDD) {	
		System.out.println("checkRecoundCount");
		
		
		return setDirtyFlag(dataRDD);
		
	}
	public Dataset<Row> setDirtyFlag(Dataset<Row> dataRDD) {
		
		
		Dataset<Row> newdataRDD = dataRDD.withColumn(FileConstants.COLUMN_NAME_DIRTY_FLAG,functions.lit(0));
		
		return newdataRDD;
		
		
	}
	
	private int performValidation(String dataFileName, JSONObject schemaFileJSON, SparkSession spark) {
		
		 // Validate Record Count header against schema
		Dataset<Row> outputRDD = checkRecoundCount(dataFileName, schemaFileJSON, spark);
		
       	outputRDD.show();
        
        
    	if(outputRDD != null ) {
        	
        	OutputWriterANZ.writeRDD(outputRDD);
    	}
    	
    	return 0;
    	
	}
	

	private Dataset<Row> checkRecoundCount(String dataFileName, JSONObject schemaFileJSON, SparkSession spark ) {
		
		// TODO Auto-generated method stub
		StructType schemaFileStructType = CodeChallengeUtil.convertJSONtoStruct(schemaFileJSON);
		
		Dataset<Row> outputRDD = InputFileReaderANZ.csvReadSQL(dataFileName, spark, schemaFileStructType);
		
		
		 // Validate Record Count header against schema
		//outputRDD = setDirtyFlag(outputRDD);
		
		outputRDD = outputRDD.withColumn(FileConstants.COLUMN_NAME_DIRTY_FLAG, functions.when(outputRDD.col(FileConstants.COLUMN_NAME_CORRUPT_RECORD).isNull(), "0").otherwise(1));
       	
		outputRDD = outputRDD.drop(FileConstants.COLUMN_NAME_CORRUPT_RECORD);

		return outputRDD;
    	
		
	}
	@Override
	public int validate( long tagCount, JSONObject schemaFileJSON, String data, String tagFileName,  SparkSession spark) {
		
		// TODO Auto-generated method stub
		System.out.println(getClass().getName());
		
		return performValidation(data, schemaFileJSON, spark  );
	}

}
