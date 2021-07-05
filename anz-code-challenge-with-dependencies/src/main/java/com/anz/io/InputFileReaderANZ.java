package com.anz.io;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class InputFileReaderANZ {
	
	
	public static Dataset<Row> csvReadSQL(String data, SparkSession spark) {
		
		
		Dataset<Row> df = spark.read().option("header",true).csv(data);
		
		df.show();
		return df;
	}

	public static JSONObject getSchemaFileJSON(String schema) {
		// TODO Auto-generated method stub
		
		JSONObject schemaFileJSON = null;   
		JSONParser jsonParser = new JSONParser();
	        
		try (FileReader reader = new FileReader(schema))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
 
            schemaFileJSON = (JSONObject) obj;
 
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

		return schemaFileJSON;
	}

	public static String[] getTagFileValues(String tag) {
		
		String[] tagString = null;
		// TODO Auto-generated method stub
		try (FileReader reader = new FileReader(tag))
        {
			Scanner sc = new Scanner(reader);
			while(sc.hasNextLine()){
			
				String str = sc.nextLine();
				tagString = str.split("\\|");
				System.out.println("tag String ==>" + tagString.length);
			}
 
        } catch (IOException e) {
            e.printStackTrace();
        }

		return tagString;
	}

	public static String getDataFileName(String data) {
		
		
		// TODO Auto-generated method stub
		File fileName = new File(data);
		//String fileName[] = data.split("\\");
		return fileName.getName();
	}


	public static Dataset<Row> readSchemaToRDD(String schemaFilePath, SparkSession spark) {
		// TODO Auto-generated method stub
		
		Dataset<Row> schemaRDD = spark.read().option("multiline", "true").json(schemaFilePath).toDF();
		
		schemaRDD.foreach(item -> {
            System.out.println(item); 
        });
		
		schemaRDD.printSchema();
		schemaRDD.show();
		
		return schemaRDD;
	}
	

}
