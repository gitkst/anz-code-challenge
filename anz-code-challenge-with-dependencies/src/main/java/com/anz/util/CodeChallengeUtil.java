package com.anz.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.anz.config.ValidationRegistery;
import com.anz.io.InputFileReaderANZ;
import com.anz.validator.ifc.ValidatorANZIfc;

public class CodeChallengeUtil {

	//java -jar anz-code-challenge-UBER.jar \
	//	-schema /path/to/schema.json \
	//	-data /path/to/data.csv \
	//	-tag /path/to/tag.tag \
	//	-output /path/to/output.csv
	
	
	public static HashMap<String, String> convertToKeyValuePair(String[] args) {

	    HashMap<String, String> params = new HashMap<>();
	    
	    for (int i = 0; i < args.length; i++) {
	    	
	    	String key, value = null;
	    			
	    	switch (args[i].charAt(0)) {
	        case '-':
	        	if ( (args.length-1 == i ) || (args[i+1].charAt(0) == '-') )
                    throw new IllegalArgumentException("Expected arg after: "+args[i]);
	        	key = args[i].substring(1);
	        	value = args[i+1];
	        	params.put(key, value);
	        	break;
	        	
        	default:
        		break;
	    	}

	    }

	    return params;
	}
	
	
	// Build the validation change by reading from Properties file
	public static List<ValidatorANZIfc> getValidatorChain(String propertyFilePath) {
		// TODO Auto-generated method stub
		
		List<ValidatorANZIfc> validationEventList = new ArrayList<ValidatorANZIfc>();
		
		String[] validationEventsArray = InputFileReaderANZ.getPropertyFileValues(propertyFilePath);
		
		for (String validationEvent : validationEventsArray) {
			
			validationEventList.add(ValidationRegistery.getValidationObject(validationEvent.trim()));
		}
		
		
		return validationEventList;
	}
	
	public static StructType convertJSONtoStruct(JSONObject schemaJSON) {
		
		StructField strF = null;
		List<StructField> strFieldList = new ArrayList<>();

		JSONArray schemaArray = (JSONArray)schemaJSON.get("columns");
		
		for (int i = 0; i < schemaArray.size(); i++) {
			
			JSONObject object = (JSONObject) schemaArray.get(i);

			System.out.println(object);
			
			strF =  DataTypes.createStructField((String)object.get("name"), 
					getDataType((String)object.get("type")),
					((Boolean)object.get("mandatory")).booleanValue(), 
					Metadata.empty());
			
			strFieldList.add(strF);
			
		} 
		
		StructType strDef = DataTypes.createStructType(strFieldList);
		
		System.out.println("strDef.defaultSize()" +strDef.size());
		
		return strDef;
	}


	private static DataType getDataType(String dataType) {
		// TODO Auto-generated method stub
		
		if(dataType.equals("STRING")) return DataTypes.StringType;
		if(dataType.equals("INTEGER")) return DataTypes.IntegerType;
		if(dataType.equals("DATE")) return DataTypes.StringType;
		
		return DataTypes.StringType;
	}
		
	
}
