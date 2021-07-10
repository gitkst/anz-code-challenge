package com.anz.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.anz.util.FileConstants;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Set;

public class InputFileReaderANZ {
	
	private static InputFileReaderANZ inputReader = new InputFileReaderANZ(); 
	
	public static Dataset<Row> csvReadSQL(String data, SparkSession spark) {
		
		
		Dataset<Row> df = spark.read().option("header",true).csv(data);
		
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

	
	public static String[] getPropertyFileValues(String propfile) {
		// TODO Auto-generated method stub
		
		System.out.println("propfile==>" + propfile);
		Properties prop = new Properties();
		String[] validationEventsArray = null;
		try (InputStream input = new FileInputStream(propfile)) {


            // load a properties file
            prop.load(input);

            System.out.println(prop);
            
            String validationSeq = (String) prop.get(FileConstants.VALIDATION_SEQ);
            validationEventsArray = validationSeq.split(",");
            
            // get the property value and print it out

        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return validationEventsArray;
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
	
	public static Dataset<Row> readSchemaToRDD(String schemaFilePath, SparkSession spark, JSONObject schemaJSON) {
		// TODO Auto-generated method stub
		
		
		JSONArray jarrya = (JSONArray)schemaJSON.get("columns");
		for (Iterator iterator = jarrya.iterator(); iterator.hasNext();) {
			JSONObject object = (JSONObject) iterator.next();
			System.out.println(object.get("name"));
			
		}
		
		
		
		Dataset<Row> schemaRDD = spark.read().option("multiline", "true").json(schemaFilePath).toDF();
		
		schemaRDD.foreach(item -> {
            System.out.println(item); 
        });
		
		schemaRDD.printSchema();
		schemaRDD.show(false);
		
		return schemaRDD;
	}
	
	
	public static Dataset<Row> readSchemaToRDD(String schemaFilePath, SparkSession spark, String dataFilePath) {
		// TODO Auto-generated method stub
		
		
		
//		spark.read()
//		.option("multiline", "true")
//		.option("inferSchema", "false")
//		.csv("");
		
		Dataset<Row> schemaRDD1 = spark.read().option("header", "true").csv(dataFilePath);
		schemaRDD1.printSchema();
		//schemaRDD1.show();
		
		Dataset<Row> schemaRDD = spark.read().option("multiline", "true")
				.json(schemaFilePath).select("columns");
				
				
				
		
		
		schemaRDD.foreach(item -> {
			
            System.out.println(item); 
        });
		
		schemaRDD.printSchema();
		schemaRDD.show(false);
		
		//Encoder<Row> rowEncoder = Encoders.bean(Row.class);
		
		
		
		//StructType strSchema = (StructType) DataType.fromJson(schemaRDD.first().prettyJson().));
		//strSchema.printTreeString();
		//Dataset<Row> schemaRDD4 = spark.read().csv
			//	(schemaRDD.head().toSeq(), rowEncoder);
		System.out.println("Hellowwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"); 
		
		
		StructType schemaDefault = makeDefaultSchema();
		schemaDefault.printTreeString();
		
		DataFrameReader dft = spark.read().schema(schemaDefault);
		
		
		System.out.println("SDD33333");
		
		Dataset<Row> schemaRDD3 = spark.read().option("header", "true").option("mode", "DROPMALFORMED").schema(schemaDefault).format("csv").load(dataFilePath);
		schemaRDD3.printSchema();
		schemaRDD3.schema().printTreeString();
		schemaRDD3.show(false);
		
		
		//schemaRDD.head().getValuesMap();
		
		
		return schemaRDD;
	}

	public static Dataset<Row> readSchemaToRDD2(String schemaFilePath, SparkSession spark, String dataFilePath) {
		// TODO Auto-generated method stub
		
		
		JSONObject schemaJson = getSchemaFileJSON(schemaFilePath);
		JSONArray schemaCol = (JSONArray) schemaJson.get("columns");
		
		System.out.println(schemaCol.toString());
		System.out.println(schemaCol.toArray().getClass());
		System.out.println(schemaCol.toJSONString());
		
		
		//StructType schemaSDD = spark.read().option("multiline", "true").json(schemaFilePath).schema().apply(   );
		//schemaSDD.printTreeString();
		
		Dataset<Row> schemaRDD4 = spark.read().option("multiline", "true").json(schemaFilePath).select("columns").toDF();
		schemaRDD4.show();
		
		convertJSONArrayToStructType(schemaCol);
		
		StructType schemaSDD2 = (StructType) DataType.fromJson((String) schemaCol.toArray()[0]);
		
		
		//StructType schemaSDD2 = new StructType((StructField[]) schemaCol.toArray());
		schemaSDD2.printTreeString();
		
		Dataset<Row> schemaRDD1 = spark.read().csv(dataFilePath);
		
		System.out.println("readSchemaToRDD ==> " );
		
		
		Dataset<Row> schemaRDD = spark.read().schema(schemaSDD2).option("header", "true").csv(dataFilePath);
		
		System.out.println("readSchemaToRDD2 ==> ");
		
		schemaRDD.foreach(item -> {
            System.out.println(item); 
        });
		
		schemaRDD.printSchema();
		schemaRDD.show();
		
		
		schemaRDD1.printSchema();
		System.out.println(" printing schema for Schema1 RDD");
		schemaRDD1.show();
		
		return schemaRDD;
	}
	
	private static void convertJSONArrayToStructType(JSONArray jsonArray) {
		
		System.out.println("convertJSONArrayToStructType");
		
		jsonArray.iterator();
		
		StructType str = new StructType();
		
		
		for (Object obj: jsonArray) {
			
			JSONObject jsonObj = (JSONObject) obj;
			System.out.println(jsonObj.get("name"));
			System.out.println(jsonObj.get("type"));
			System.out.println(jsonObj.get("format"));
			System.out.println(jsonObj.get("mandatory"));
			
			//StringType.;
			
			//StructField strField = new StructField((String)jsonObj.get("name"), null, false, null);
			
			//str.add(strField);
		}
		
		str.printTreeString();
		
		
		
	}
	
	private static StructType makeDefaultSchema() {
		
		StructType strDef = new StructType()
				.add(new StructField("State/Territory", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Capital", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Population", DataTypes.IntegerType, true, Metadata.empty()));
				//.add(new StructField("Established", DataTypes.DateType, true, Metadata.empty()));
		
		
			
		return strDef;
	}

	public static InputFileReaderANZ getInstance() {
		
		return inputReader;
	}

	public static Dataset<Row> csvReadSQL(String data, SparkSession spark, StructType schemaFileStructType) {
		// TODO Auto-generated method stub
		
		List<StructField> abc = new ArrayList<>();
		abc.add(DataTypes.createStructField(FileConstants.COLUMN_NAME_CORRUPT_RECORD, DataTypes.StringType, true));
		
		StructType schemaFileStructType2 = DataTypes.createStructType(abc);
		
		schemaFileStructType = schemaFileStructType.merge(schemaFileStructType2);
		
		System.out.println("checck Drop the ====================>");
		schemaFileStructType.printTreeString();
		
		Dataset<Row> df = spark.read().schema(schemaFileStructType).
				option("columnNameOfCorruptRecord",FileConstants.COLUMN_NAME_CORRUPT_RECORD)
				.option("enforceSchema", false)
				.option("mode", "PERMISSIVE")
			    .option("header",true).csv(data).cache();
		
		//.option("mode", "DROPMALFORMED")
		//df.show();
		
		//Dataset<Row> newdf = df.cache()
		
		//df.filter(df.col(FileConstants.COLUMN_NAME_CORRUPT_RECORD).isNotNull()).show();
		
		return df;
		
	}
	
}
