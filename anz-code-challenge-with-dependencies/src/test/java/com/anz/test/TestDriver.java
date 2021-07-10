package com.anz.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

import com.anz.Driver;
import com.anz.TestRegex;
import com.anz.config.ValidationRegistery;
import com.anz.io.InputFileReaderANZ;
import com.anz.util.CodeChallengeUtil;
import com.anz.util.FileConstants;
import com.anz.validator.PrimaryKeyValidator;
import com.anz.validator.ifc.ValidatorANZIfc;

public class TestDriver {
	
	
//	
//	Feature: File Integrity checks
//	These checks are typically performed to ensure that the file
//	received from a source system is wholly complete.
//	Scenario: Invalid file – file name does not match
//	Given I have a DATA file named “scenarios/aus-capitals.csv”
//	And I have a TAG file named “scenarios/aus-capitals-invalid-2.tag”
//	And I have a SCHEMA file named “scenarios/aus-capitals.json”
//	When I execute the application with output “output/sbe-1-3.csv”
//	Then the program should exit with RETURN CODE of “2”
	
   
	
    public void check_File_Name() {
    	
//		Given I have a DATA file named “scenarios/aus-capitals.csv”
//		And I have a TAG file named “scenarios/aus-capitals-invalid-2.tag”
//		And I have a SCHEMA file named “scenarios/aus-capitals.json”
//		Then the program should exit with RETURN CODE of “2”

		
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-invalid-2.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";
    	String output = "//path//aus-capitals-output2.csv";
		String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
        
		List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
    	int valCheck_File_Name = Driver.performValidation(schema, data, tag, output, validationEventList );
    	
        assertThat(valCheck_File_Name).isEqualTo(2);
        
    }
    
	@Test
    public void check_primaryKey() {
    	
//    	Given I have a DATA file named “scenarios/aus-capitals-dupes.csv”
//    	And I have a TAG file named “scenarios/aus-capitals.tag”
//    	And I have a SCHEMA file named “scenarios/aus-capitals.json”
//    	When I execute the application with output “output/sbe-1-4.csv”
//    	Then the program should exist with RETURN CODE of 3
    	
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-dupes.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";
    	
    	String output = "//path//aus-capitals-output2.csv";
		String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
		
		List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		List<ValidatorANZIfc> validationEventList2 = new ArrayList<>();
		validationEventList2.add(ValidationRegistery.getValidationObject(FileConstants.PRIMARY_KEY_VALIDATOR));
		validationEventList2.addAll(validationEventList); 
		
         int isPrimaryKeyUnique = Driver.performValidation(schema, data, tag, output, validationEventList2 );
    	
        assertThat(isPrimaryKeyUnique).isEqualTo(3);
        
    }
    
    
    public void check_RecordCcount() {
    	
//    	Given I have a DATA file named “scenarios/aus-capitals.csv”
//    	And I have a TAG file named “scenarios/aus-capitals-invalid-1.tag”
//    	And I have a SCHEMA file named “scenarios/aus-capitals.json”
//    	When I execute the application with output “output/sbe-1-2.csv”
//    	Then the program should exit with RETURN CODE of “1”
//    	
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-invalid-1.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";

    	String output = "//path//aus-capitals-output2.csv";
		
    	String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
		
    	List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		List<ValidatorANZIfc> validationEventList2 = new ArrayList<>();
		validationEventList2.add(ValidationRegistery.getValidationObject(FileConstants.PRIMARY_KEY_VALIDATOR));
		validationEventList2.addAll(validationEventList); 
         
         int valcheck_RecordCcount = Driver.performValidation(schema, data, tag, output, validationEventList );
        
        assertThat(valcheck_RecordCcount).isEqualTo(1);
        
        
    }
    
    
    public void check_Valid_Case_Scenario() {
    	
//		Scenario: Valid file – all checks are successful
//		Given I have a DATA file named “scenarios/aus-capitals.csv”
//		And I have a TAG file named “scenarios/aus-capitals.tag”
//		And I have a SCHEMA file named “scenarios/aus-capitals.json”
//		When I execute the application with output “output/sbe-1-1.csv”
//		Then the program should exit with RETURN CODE of “0”

		
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";
    	String output = "output/sbe-1-1.csv";


		String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
		
		List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		List<ValidatorANZIfc> validationEventList2 = new ArrayList<>();
		validationEventList2.add(ValidationRegistery.getValidationObject(FileConstants.PRIMARY_KEY_VALIDATOR));
		validationEventList2.addAll(validationEventList); 
		
		 int valCheck_Valid_Case_Scenario = Driver.performValidation(schema, data, tag, output, validationEventList );
    	
        assertThat(valCheck_Valid_Case_Scenario).isEqualTo(0);
        
    }
	
	@Test
    public void check_Missing_columns() {
    	
//		Scenario: Invalid file – missing columns
//		Given I have a DATA file named “scenarios/aus-capitals-missing.csv”
//		And I have a TAG file named “scenarios/aus-capitals.tag”
//		And I have a SCHEMA file named “scenarios/aus-capitals.json”
//		When I execute the application with output “output/sbe-1-5.csv”
//		Then the program should exist with RETURN CODE of 4

		
		String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-missing.csv";
		String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.tag";
		String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";
		String output = "output/sbe-1-5.csv";

		String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";

		List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		List<ValidatorANZIfc> validationEventList2 = new ArrayList<>();
		validationEventList2.add(ValidationRegistery.getValidationObject(FileConstants.COLUMN_VALIDATOR));
		validationEventList2.addAll(validationEventList); 
		
		int missing_file_scenario = Driver.performValidation(schema, data, tag, output, validationEventList2);

		assertThat(missing_file_scenario).isEqualTo(4);
        
    }
   
   
	@Test
    public void check_Additonal_columns() {
    	
//		Scenario: Invalid file – additional columns
//		Given I have a DATA file named “scenarios/aus-capitals-addition.csv”
//		And I have a TAG file named “scenarios/aus-capitals.tag”
//		And I have a SCHEMA file named “scenarios/aus-capitals.json”
//		When I execute the application with output “output/sbe-1-6.csv”
//		Then the program should exist with RETURN CODE of 4

		
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-addition.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";
    	String output = "output/sbe-1-6.csv";

    	String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
    	
    	List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		List<ValidatorANZIfc> validationEventList2 = new ArrayList<>();
		validationEventList2.add(ValidationRegistery.getValidationObject(FileConstants.COLUMN_VALIDATOR));
		validationEventList2.addAll(validationEventList); 
		
		 int additional_file_scenario = Driver.performValidation(schema, data, tag, output, validationEventList2 );
   	
       assertThat(additional_file_scenario).isEqualTo(4);
       
        
    }
	
    @Test
    public void check_Invalid_Scenario() {
    	
//    	Scenario: Invalid fields
//    	Given I have a DATA named “scenarios/aus-capitals-invalid-3.csv”
//    	And I have a TAG file named “scenarios/aus-capitals.tag”
//    	And I have a SCHEMA file named “scenarios/aus-capitals.json”
//    	When I execute the application with output “output/act-sbe2-2.csv”
//    	Then the program should exist with RETURN CODE of 0
//    	And “output/act-sbe2-2” should match “scenarios/exp-sbe2-2.csv”
//    	
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-invalid-3.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-invalid-1.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";

    	String output = "//path//aus-capitals-output2.csv";
		
    	String propertyFile = "C:\\Users\\kaush\\Documents\\git\\repository\\anz-code-challenge-with-dependencies\\src\\main\\resources\\fileValidation.properties";
		
    	List<ValidatorANZIfc> validationEventList = CodeChallengeUtil.getValidatorChain(propertyFile);
		
		List<ValidatorANZIfc> validationEventList2 = new ArrayList<>();
		validationEventList2.add(ValidationRegistery.getValidationObject(FileConstants.FIELD_VALIDATOR));
		//validationEventList2.addAll(validationEventList); 
		
         int check_Invalid_Scenario = Driver.performValidation(schema, data, tag, output,  validationEventList2);
        
         assertThat(check_Invalid_Scenario).isEqualTo(0);
        
        
    }

}
