package com.anz.io;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.anz.util.FileConstants;

public class OutputWriterANZ {

	public static void writeRDD(Dataset<Row> outputRDD, String outputFileName ) {
		// TODO Auto-generated method stub
		
		System.out.println("Write File " + outputFileName);
		outputRDD.write().format("parquet").option("header","true").mode(SaveMode.Overwrite).save(FileConstants.outputPath+outputFileName);
		
	}
	
	public static void writeExmptyFile( String outputFileName) {
		// TODO Auto-generated method stub
		
		System.out.println("Write writeExmptyFile " + outputFileName);
		FileWriter fw = null;
		try {
			
			fw = new FileWriter(FileConstants.outputPath + outputFileName);
			fw.write("");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
		
		
		
	}

}
