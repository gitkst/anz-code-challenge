package com.anz.validator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;

import com.anz.io.InputFileReaderANZ;
import com.anz.io.OutputWriterANZ;
import com.anz.util.FileConstants;
import com.anz.validator.ifc.ValidatorANZIfc;

public class FileNameValidator implements ValidatorANZIfc {
	
	
	public boolean isDataFileNameSameAsTag(String dataFileName, String tagFileName) {
	
		//Matches the last extension of csv and .tag file name. 
		// $ makes sure the .csv and .tag is at the end
		
		//String dataFN = dataFileName.split(".csv$")[0];
		//String tagFN = tagFileName.split(".tag$")[0];
		System.out.println("dataFileName"+ dataFileName +"tagFileName"+ tagFileName);
		return dataFileName.equals(tagFileName);
		
	}
	
	
	
	private int performValidation(String dataFileName, String tagFileName) {
		
		
		
		System.out.println("dataFileNameCheck == Started");
    	boolean isDataFileNameSameAsTag = isDataFileNameSameAsTag(dataFileName, tagFileName);
    	System.out.println("dataFileNameCheck == Started" + isDataFileNameSameAsTag);
    	
    	if(!isDataFileNameSameAsTag) {
    		
	    	OutputWriterANZ.writeExmptyFile(FileConstants.FILE_NAME_DATA_FILE_NAME_CHECK_INVALID);
	    	return 2;
    	}
    	


		return 0;
    	
	}

	@Override
	public int validate( long tagCount, JSONObject schemaFileJSON, String data, String tagFileName,  SparkSession spark) {
		
		// TODO Auto-generated method stub
		System.out.println(getClass().getName());
		
		String dataFileName = InputFileReaderANZ.getDataFileName(data);
		
		return performValidation(dataFileName, tagFileName);
	}

}
