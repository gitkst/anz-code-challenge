package com.anz.junk;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;

import com.anz.io.InputFileReaderANZ;
import com.anz.io.OutputWriterANZ;
import com.anz.util.CodeChallengeUtil;
import com.anz.util.FileConstants;
import com.anz.validator.ColumnValidator;
import com.anz.validator.FieldValidator;
import com.anz.validator.FileNameValidator;
import com.anz.validator.PrimaryKeyValidator;
import com.anz.validator.RecordCountValidator;
import com.anz.validator.ifc.ValidatorANZIfc;

public class Driver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println("Hello");
		
		
		// Read Arguments in Key Value Pair
		HashMap<String, String> params = CodeChallengeUtil.convertToKeyValuePair(args);

		params.forEach((k, v) -> System.out.println("Key : " + k + " Value : " + v));
		
		String schema = params.get("schema");
		String data = params.get("data");
		String tag = params.get("tag");
		String output = params.get("output");
		String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
		
		
		int returnCode = performValidation(schema, data, tag, output, propertyFile );
		
		//returnCode = callMainDriverProgram(schema, data, tag, output) ;
		
		
        
	}
	
	public static int performValidation(String schema, String data, String tag, String output, String propertyFile ) {
		
		int returnCode = 0;
		
		// Prepare the validation Sequence of Event
		List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		
		// Read Data from files
		System.out.println("Hello **************************** Create Spark Conf ");
        SparkSession spark = SparkSession.builder().appName("Driver").config("spark.master", "local").getOrCreate();
        
        
        // Read Files into memory
        String dataFileName = InputFileReaderANZ.getDataFileName(data);
        Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark); 
        
        System.out.println("Hello ===================> 123");
        
        String[] tagValue = InputFileReaderANZ.getTagFileValues(tag);
        JSONObject schemaFileJSON = InputFileReaderANZ.getSchemaFileJSON(schema);
        Dataset<Row> schemaFileRDD = InputFileReaderANZ.readSchemaToRDD(schema, spark);
        Dataset<Row> outputRDD = null;
        String outputFN = InputFileReaderANZ.getDataFileName(output);
        //int counter = 10;
        long tagCount = Long.valueOf(tagValue[1]).longValue();
				
		
        
        // Perform Dynamic Validation
		for (Iterator iterator = validationEventList.iterator(); iterator.hasNext();) {
			
			ValidatorANZIfc validatorANZIfc = (ValidatorANZIfc) iterator.next();
			
			System.out.println("Hello ===================> 123" + validatorANZIfc.getClass().getName());
	        	
			
			//returnCode = validatorANZIfc.validate( dataRDD, tagCount , schemaFileJSON, dataFileName, tagValue[0], schemaFileRDD );

			if(returnCode != 0)  break;
			
			
		}
		
		spark.stop();
		
		return returnCode;
	}
	

	public static int callMainDriverProgram(String schema, String data, String tag, String output) {
		// TODO Auto-generated method stub
		
		System.out.println("Hello **************************** Create Spark Conf ");
        SparkSession spark = SparkSession.builder().appName("Driver").config("spark.master", "local").getOrCreate();
        
        
        // Read Files into memory
        String dataFileName = InputFileReaderANZ.getDataFileName(data);
        Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark); 
        
        System.out.println("Hello ===================> 123");
        
        String[] tagValue = InputFileReaderANZ.getTagFileValues(tag);
        JSONObject schemaFileJSON = InputFileReaderANZ.getSchemaFileJSON(schema);
        Dataset<Row> schemaFileRDD = InputFileReaderANZ.readSchemaToRDD(schema, spark);
        Dataset<Row> outputRDD = null;
        String outputFN = InputFileReaderANZ.getDataFileName(output);
        //int counter = 10;
        long tagCount = Long.valueOf(tagValue[1]).longValue();
        
        
        
        // Boolean Values
        
        //int return codes
        int primaryKeyReturnCode = 0;
        int recordCountReturnCode = 0;
        int dataFileNameSameAsTagReturnCode = 0;
        int doesSchemaMatchsCSVReturnCode = 0;
        
	    // 1. Validate Record Count
    	recordCountReturnCode = recordCountCheck(dataRDD, tagCount);

    	// 2. Validate the Data File Name matches the Tag | tagValue[0] covers the name.
	    if(recordCountReturnCode == 0 ) {
	    
	    	dataFileNameSameAsTagReturnCode = dataFileNameCheck(dataFileName, tagValue[0]);

        }
        
        
        	
        // 3. Validate the uniqueness of Primary Key with Schema
        if(dataFileNameSameAsTagReturnCode == 0 ) {
        	
        	primaryKeyReturnCode = primaryKeyCheck(schemaFileJSON, dataRDD);
        }
        
        	
        // Validate csv header against schema
        if(primaryKeyReturnCode == 0 ) {
        	
        	doesSchemaMatchsCSVReturnCode = doesSchemaMatchsCSV(dataRDD, schemaFileRDD);
        		
        }
        
        	
        // Validate Record Count header against schema
        if(doesSchemaMatchsCSVReturnCode == 0 ) {
        	
        	outputRDD = new FieldValidator().checkRecoundCount(dataRDD);
        	outputRDD.show();
        }
        
        
    	if(outputRDD != null ) {
        	
        	OutputWriterANZ.writeRDD(outputRDD, outputFN);
    	}
            
        
       
        spark.stop();
        //sparkContext.close();
        return ( primaryKeyReturnCode + recordCountReturnCode + dataFileNameSameAsTagReturnCode + doesSchemaMatchsCSVReturnCode );

	}

	
	public static int doesSchemaMatchsCSV(Dataset<Row> dataRDD, Dataset<Row> schemaFileRDD) {
		// TODO Auto-generated method stub
		
		System.out.println("doesSchemaMatchsCSV == Started");
		
		boolean[] doesSchemaMatchsCSV = new ColumnValidator().doesSchemaMatchsCSV(dataRDD, schemaFileRDD);
		
		System.out.println("doesSchemaMatchsCSV == Started" + doesSchemaMatchsCSV[0] + doesSchemaMatchsCSV[1]);
		
    	if(! doesSchemaMatchsCSV[0]) {
    		
    		String outputFileName = FileConstants.FILE_NAME_MISSING_COLUMNS;
    		if (doesSchemaMatchsCSV[1]) {
    			
    			outputFileName = FileConstants.FILE_NAME_ADDITIONAL_COLUMNS;
    		}
    		
    		OutputWriterANZ.writeExmptyFile(outputFileName);
    		return 0;
    		
    	}
    	
		return 0;
	}

	public static int dataFileNameCheck(String dataFileName, String tagFileName) {
		// TODO Auto-generated method stub
		
		System.out.println("dataFileNameCheck == Started");
    	boolean isDataFileNameSameAsTag = new FileNameValidator().isDataFileNameSameAsTag(dataFileName, tagFileName);
    	System.out.println("dataFileNameCheck == Started" + isDataFileNameSameAsTag);
    	
    	if(!isDataFileNameSameAsTag) {
    		
	    	OutputWriterANZ.writeExmptyFile(FileConstants.FILE_NAME_DATA_FILE_NAME_CHECK_INVALID);
	    	return 2;
    	}
    	


		return 0;
	}

	public static int primaryKeyCheck(JSONObject schemaFileJSON, Dataset<Row> dataRDD) {
		
		System.out.println("primaryKeyCheck == Started");
		boolean isPrimaryKeyUnique = new PrimaryKeyValidator().isPrimaryKeyUnique(schemaFileJSON, dataRDD);
		System.out.println("primaryKeyCheck == Started" + isPrimaryKeyUnique);
		
    	if(!isPrimaryKeyUnique) {
    		
	    	OutputWriterANZ.writeExmptyFile(FileConstants.FILE_NAME_PRIMARY_KEY_CHECK_INVALID);
	    	return 3;
    	}
    	
    	return 0;
    	
    	
	}
	
	public static int recordCountCheck(Dataset<Row> dataRDD, long tagCount) {
		
		System.out.println("recordCountCheck == Started");
	    boolean isRecordCountSameAsTag = new RecordCountValidator().isRecordCountSameAsTag(dataRDD, tagCount);
	    System.out.println("recordCountCheck == Started"+ isRecordCountSameAsTag);
		
    	if(! isRecordCountSameAsTag) {
    		
	    	OutputWriterANZ.writeExmptyFile(FileConstants.FILE_NAME_RECORD_COUNT_CHECK_INVALID);
	    	return 1;
    	}
    	
    	return 0;
    	
    	
	}
	
	



}
