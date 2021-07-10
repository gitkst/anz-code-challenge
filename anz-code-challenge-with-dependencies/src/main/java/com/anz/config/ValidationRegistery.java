package com.anz.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;

import com.anz.util.FileConstants;
import com.anz.validator.ColumnValidator;
import com.anz.validator.FieldValidator;
import com.anz.validator.FileNameValidator;
import com.anz.validator.PrimaryKeyValidator;
import com.anz.validator.RecordCountValidator;
import com.anz.validator.ifc.ValidatorANZIfc;

public class ValidationRegistery {
	
	private static Map<String, ValidatorANZIfc> validationRegistry = new HashedMap();
	///<ValidatorANZIfc> validationRegistry = new ArrayList<ValidatorANZIfc>();
	
	static{
		validationRegistry.put(FileConstants.PRIMARY_KEY_VALIDATOR, new PrimaryKeyValidator() );
		validationRegistry.put(FileConstants.RECORD_COUNT_VALIDATOR, new RecordCountValidator() );
		validationRegistry.put(FileConstants.FILE_NAME_VALIDATOR, new FileNameValidator() );
		validationRegistry.put(FileConstants.COLUMN_VALIDATOR, new ColumnValidator() );
		validationRegistry.put(FileConstants.FIELD_VALIDATOR, new FieldValidator() );
	}

	public static ValidatorANZIfc getValidationObject(String validationEvent) {
		// TODO Auto-generated method stub
		return validationRegistry.get(validationEvent);
	}

}
