package com.anz.validator;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;

import com.anz.io.InputFileReaderANZ;
import com.anz.io.OutputWriterANZ;
import com.anz.util.CodeChallengeUtil;
import com.anz.util.FileConstants;
import com.anz.validator.ifc.ValidatorANZIfc;

public class ColumnValidator implements ValidatorANZIfc {
	
	public boolean[] doesSchemaMatchsCSV( Dataset<Row> dataRDD, Dataset<Row> schemaFileRDD) {
		//Print Schema
		schemaFileRDD.printSchema();
		dataRDD.printSchema();
		
		StructType schemaFileStruct = schemaFileRDD.schema();
		StructType dataFileStruct = dataRDD.schema();
		
		return isSchemaDiff(schemaFileStruct, dataFileStruct);
	}
	
	private boolean[] isSchemaDiff (StructType schemaFileStruct, StructType dataFileStruct) {
		
		StructField[] schemaFileStructArray  = schemaFileStruct.fields();
		StructField[] dataFileStructArray  = dataFileStruct.fields();
		
		System.out.println("schemaFileStructArray ==>"+ schemaFileStructArray.length + "dataFileStructArray==>" + dataFileStructArray.length);
		
		boolean[] schemaMatch = {true, false};
		
		
		if(schemaFileStructArray.length < dataFileStructArray.length ) {
			
			//additonal fields -> Hence this is set to true
			schemaMatch[1] = true;
			
		}
		
		for(int i=0;  i < schemaFileStructArray.length ; i++) {
			
			if(! schemaFileStructArray[i].equals(dataFileStructArray[i])) {
				schemaMatch[0] = false;
			}
		}
		
		return schemaMatch;
		
	}
	
	/*
	private Map getCleanedSchema(DataFrame df): Map[String, (DataType, Boolean)] = {
		    df.schema.map { (structField: StructField) =>
		      structField.name.toLowerCase -> (structField.dataType, structField.nullable)
		    }.toMap
		  }
		  
		  import org.apache.spark.sql.types.{StructType, StructField}

val schemaDiff = (s1 :StructType, s2 :StructType) => {
      val s1Keys = s1.map{_.toString}.toSet
      val s2Keys = s2.map{_.toString}.toSet
      val commonKeys =  s1Keys.intersect(s2Keys)

      val diffKeys = s1Keys ++ s2Keys -- commonKeys

      (s1 ++ s2).filter(sf => diffKeys.contains(sf.toString)).toList.
}

	*/

	private boolean isSchemaSame (StructType schemaFileStruct, StructType dataFileStruct) {
		
		StructField[] schemaFileStructArray  = schemaFileStruct.fields();
		StructField[] dataFileStructArray  = dataFileStruct.fields();
		
			
		for(int i=0;  i < schemaFileStructArray.length ; i++) {
			
			boolean nameDiff =  schemaFileStructArray[i].name().equals(dataFileStructArray[i].name());
			boolean typeDiff =  schemaFileStructArray[i].dataType().equals(dataFileStructArray[i].dataType());
			boolean nullable =  (schemaFileStructArray[i].nullable() == dataFileStructArray[i].nullable());
			
			if( !nameDiff &&  !nullable ) {
				return false;
				
			}
			
		}
							
		return true;
		
	}
	
	private int getSchemaFieldLengthDiff(StructType dataFileStructType, StructType schemaFileStructType) {
		// TODO Auto-generated method stub
		return dataFileStructType.size() - schemaFileStructType.size();
	}

	
	private int performValidation(Dataset<Row> dataRDD, JSONObject schemaFileJSON) {
		
		System.out.println("doesSchemaMatchsCSV == Started");
		StructType schemaFileStructType = CodeChallengeUtil.convertJSONtoStruct(schemaFileJSON);
		
		int fieldlengthDifference =  getSchemaFieldLengthDiff(dataRDD.schema(), schemaFileStructType);
		
		String outputFileName = null;
		
		if( fieldlengthDifference > 0 ) {
			
			outputFileName = FileConstants.FILE_NAME_ADDITIONAL_COLUMNS;
			OutputWriterANZ.writeExmptyFile(outputFileName);
			return 4;
			
		} else if (fieldlengthDifference < 0) {
		
			outputFileName = FileConstants.FILE_NAME_MISSING_COLUMNS;
			OutputWriterANZ.writeExmptyFile(outputFileName);
			return 4;
			
		} else {
		
			boolean isSchemaSame = isSchemaSame(dataRDD.schema(), schemaFileStructType);
			
			if( !isSchemaSame ) {
				outputFileName = FileConstants.FILE_NAME_COLUMNS_NOT_MATCH;
				OutputWriterANZ.writeExmptyFile(outputFileName);
				return 5;
			}
		}
				
		return 0;
    	
	}

	
	@Override
	public int validate( long tagCount, JSONObject schemaFileJSON, String data, String tagFileName,  SparkSession spark) {
		
		// TODO Auto-generated method stub
		System.out.println(getClass().getName());
		
		Dataset<Row> dataRDD = InputFileReaderANZ.csvReadSQL(data, spark);
		
		return performValidation(dataRDD, schemaFileJSON);
	}
}
