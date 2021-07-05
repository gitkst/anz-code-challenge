package com.anz.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

import com.anz.Driver;
import com.anz.TestRegex;
import com.anz.io.InputFileReaderANZ;
import com.anz.validator.PrimaryKeyValidator;

public class TestDriver {
	
	
   
	
    public void check_File_Name() {
    	
//		Given I have a DATA file named “scenarios/aus-capitals.csv”
//		And I have a TAG file named “scenarios/aus-capitals-invalid-1.tag”
//		And I have a SCHEMA file named “scenarios/aus-capitals.json”
//		Then the program should exit with RETURN CODE of “2”

		
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-invalid-1.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";
    	
    	 String dataFileName = InputFileReaderANZ.getDataFileName(data);
         String[] tagValue = InputFileReaderANZ.getTagFileValues(tag);
            	
    	
    	int valCheck_File_Name = Driver.dataFileNameCheck(dataFileName, tagValue[0]);
    	
        assertThat(valCheck_File_Name).isEqualTo(2);
        
    }
    
    
    public void check_primaryKey() {
    	
//    	Given I have a DATA file named “scenarios/aus-capitals-dupes.csv”
//    	And I have a TAG file named “scenarios/aus-capitals.tag”
//    	And I have a SCHEMA file named “scenarios/aus-capitals.json”
//    	When I execute the application with output “output/sbe-1-4.csv”
//    	Then the program should exist with RETURN CODE of 3
    	
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-dupes.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";
        SparkSession spark = SparkSession.builder().appName("Driver").config("spark.master", "local").getOrCreate();

    	Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark);
    	
    	
    	JSONObject schemaFileJSON = InputFileReaderANZ.getSchemaFileJSON(schema);
    	
    	int isPrimaryKeyUnique = Driver.primaryKeyCheck(schemaFileJSON, dataRDD);
    	
    	spark.close();
    	
        assertThat(isPrimaryKeyUnique).isEqualTo(3);
        
    }
    
    
    public void check_RecordCcount() {
    	
//    	iven I have a DATA file named “scenarios/aus-capitals.csv”
//    	And I have a TAG file named “scenarios/aus-capitals-invalid-1.tag”
//    	And I have a SCHEMA file named “scenarios/aus-capitals.json”
//    	When I execute the application with output “output/sbe-1-2.csv”
//    	Then the program should exit with RETURN CODE of “1”
//    	
    	String data = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.csv";
    	String tag = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals-invalid-1.tag";
    	String schema = "E:\\Datathom\\anz\\ANZ Coding Challenge Apr 2021\\ANZ Coding Challenge Apr 2021\\scenarios\\aus-capitals.json";

    	SparkSession spark = SparkSession.builder().appName("Driver").config("spark.master", "local").getOrCreate();

    	Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark);
    	
        String[] tagValue = InputFileReaderANZ.getTagFileValues(tag);
        long tagCount = Long.valueOf(tagValue[1]).longValue();


    	int valcheck_RecordCcount = Driver.recordCountCheck(dataRDD, tagCount);
   
     	spark.close();
        
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
    	
    	
    	int valCheck_Valid_Case_Scenario = Driver.callMainDriverProgram(schema, data, tag, output);
    	
        assertThat(valCheck_Valid_Case_Scenario).isEqualTo(0);
        
    }
	
	@Test
    public void check_Schema_Matches() {
    	
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

    	SparkSession spark = SparkSession.builder().appName("Driver").config("spark.master", "local").getOrCreate();
    	Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark);
    	Dataset<Row> schemaFileRDD = InputFileReaderANZ.readSchemaToRDD(schema, spark);

    	
    	int valCheck_Valid_Case_Scenario = Driver.doesSchemaMatchsCSV(dataRDD, schemaFileRDD);

    	spark.close();
    	
        assertThat(valCheck_Valid_Case_Scenario).isEqualTo(0);
        
    }
   
   


}
