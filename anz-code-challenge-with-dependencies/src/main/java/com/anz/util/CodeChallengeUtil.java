package com.anz.util;

import java.util.HashMap;

public class CodeChallengeUtil {

	//java -jar anz-code-challenge-UBER.jar \
	//	-schema /path/to/schema.json \
	//	-data /path/to/data.csv \
	//	-tag /path/to/tag.tag \
	//	-output /path/to/output.csv
	
	
	public static HashMap<String, String> convertToKeyValuePair(String[] args) {

	    HashMap<String, String> params = new HashMap<>();
	    
	    for (int i = 0; i < args.length; i++) {
	    	
	    	String key, value = null;
	    			
	    	switch (args[i].charAt(0)) {
	        case '-':
	        	if ( (args.length-1 == i ) || (args[i+1].charAt(0) == '-') )
                    throw new IllegalArgumentException("Expected arg after: "+args[i]);
	        	key = args[i].substring(1);
	        	value = args[i+1];
	        	params.put(key, value);
	        	break;
	        	
        	default:
        		break;
	    	}

	    }

	    return params;
	}

}
