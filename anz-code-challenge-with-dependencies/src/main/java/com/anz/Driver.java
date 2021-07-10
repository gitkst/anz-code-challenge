package com.anz;


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
import org.apache.spark.sql.types.StructType;
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
		String propertyFile = params.get("property");
		//String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
		
		// Prepare the validation Sequence of Event
		List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
				
		
		int returnCode = performValidation(schema, data, tag, output, validationEventList );
		
		//returnCode = callMainDriverProgram(schema, data, tag, output) ;
		
		
        
	}
	
	public static int performValidation(String schema, String data, String tag, String output, List<ValidatorANZIfc> validationEventList ) {
		
		int returnCode = 0;
		
		// Prepare the validation Sequence of Event
		//List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		
		// Read Data from files
		System.out.println("Hello **************************** Create Spark Conf ");
        SparkSession spark = SparkSession.builder().appName("Driver").config("spark.master", "local").getOrCreate();
        
        
        // Read Files into memory
       // String dataFileName = InputFileReaderANZ.getDataFileName(data);
        //Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark); 
        
        System.out.println("Hello ===================> 123");
        
        String[] tagValue = InputFileReaderANZ.getTagFileValues(tag);
        JSONObject schemaFileJSON = InputFileReaderANZ.getSchemaFileJSON(schema);
        // not needed
        //Dataset<Row> schemaFileRDD = InputFileReaderANZ.readSchemaToRDD(schema, spark);
        //Dataset<Row> outputRDD = null;
        //String outputFN = InputFileReaderANZ.getDataFileName(output);
        //int counter = 10;
        long tagCount = Long.valueOf(tagValue[1]).longValue();
		
        // Perform Dynamic Validation
		for (Iterator iterator = validationEventList.iterator(); iterator.hasNext();) {
			
			ValidatorANZIfc validatorANZIfc = (ValidatorANZIfc) iterator.next();
			
			System.out.println("Hello ===================> 123" + validatorANZIfc.getClass().getName());
	        	
			
			returnCode = validatorANZIfc.validate(  tagCount , schemaFileJSON, data, tagValue[0], spark );

			if(returnCode != 0)  break;
			
			
		}
		
		spark.stop();
		
		return returnCode;
	}
	
	
}
