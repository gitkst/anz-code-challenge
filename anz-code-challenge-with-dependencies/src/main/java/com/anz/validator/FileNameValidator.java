package com.anz.validator;

public class FileNameValidator {
	
	
	public boolean isDataFileNameSameAsTag(String dataFileName, String tagFileName) {
	
		//Matches the last extension of csv and .tag file name. 
		// $ makes sure the .csv and .tag is at the end
		
		//String dataFN = dataFileName.split(".csv$")[0];
		//String tagFN = tagFileName.split(".tag$")[0];
		System.out.println("dataFileName"+ dataFileName +"tagFileName"+ tagFileName);
		return dataFileName.equals(tagFileName);
		
	}

}
