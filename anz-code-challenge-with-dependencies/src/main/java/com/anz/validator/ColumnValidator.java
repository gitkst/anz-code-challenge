package com.anz.validator;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ColumnValidator {
	
	public boolean[] doesSchemaMatchsCSV( Dataset<Row> dataRDD, Dataset<Row> schemaFileRDD) {
		//Print Schema
		schemaFileRDD.printSchema();
		dataRDD.printSchema();
		
		StructType schemaFileStruct = schemaFileRDD.schema();
		StructType dataFileStruct = dataRDD.schema();
		
		return isSchemaDiff(schemaFileStruct, dataFileStruct);
	}
	
	private boolean[] isSchemaDiff (StructType s1, StructType s2) {
		
		StructField[] sf1  = s1.fields();
		StructField[] sf2  = s2.fields();
		
		
		
		System.out.println("sf1 ==>"+ sf1.length + "sf2==>" + sf2.length);
		boolean[] schemaMatch = {true, false};
		
		if(sf1.length < sf2.length ) {
			//additonal fields is set is second array
			schemaMatch[1] = true;
			
			
		}
		
		for(int i=0;  i < sf1.length ; i++) {
			
			if(! sf1[i].equals(sf2[i])) {
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

      (s1 ++ s2).filter(sf => diffKeys.contains(sf.toString)).toList
}

	*/

}
