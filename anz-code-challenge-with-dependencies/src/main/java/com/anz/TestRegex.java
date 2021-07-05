package com.anz;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.anz.io.InputFileReaderANZ;
import com.anz.io.OutputWriterANZ;
import com.anz.util.CodeChallengeUtil;


public class TestRegex {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//Dataset<Row> ds1 =
		//-schema /path/to/schema.json \
//		-data /path/to/data.csv \
//		-tag /path/to/tag.tag \
//		-output /path/to/output.csv
		HashMap<String, String> params = CodeChallengeUtil.convertToKeyValuePair(args);

	    params.forEach((k, v) -> System.out.println("Key : " + k + " Value : " + v));
	    
		System.out.println(args[0] + "," + args[1]);
		
		//String tags = OutputWriterANZ.writeRDD();

		//System.out.println(tags);
	}
	
		
	private static void readEmployeeObject() {
		
		//JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();
         
        try (FileReader reader = new FileReader("E:\\workspace4.4\\anz-code-challenge-with-dependencies\\src\\main\\java\\com\\anz\\aus-capitals.json"))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
 
            JSONObject employeeList = (JSONObject) obj;
            JSONArray primaryKeys = (JSONArray) employeeList.get("primary_keys");
            System.out.println(primaryKeys.size());
             
            //Iterate over employee array
            //employeeList.forEach( emp -> parseEmployeeObject( (JSONObject) emp ) );
 
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
	}

	 private static void parseEmployeeObject(JSONObject employee) 
	    {
	        //Get employee object within list
	        JSONObject employeeObject = (JSONObject) employee.get("employee");
	         
	        //Get employee first name
	        String firstName = (String) employeeObject.get("firstName");    
	        System.out.println(firstName);
	         
	        //Get employee last name
	        String lastName = (String) employeeObject.get("lastName");  
	        System.out.println(lastName);
	         
	        //Get employee website name
	        String website = (String) employeeObject.get("website");    
	        System.out.println(website);
	    }
	

}
