package org.xululabs.datasources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class UtilFunctions {
	
	public static void main(String args[]){
//		
//		List<String> list = new ArrayList<String>();
//		list.add("100-=4 Main Differences between #Startup & #ScaleUp #Marketing");
//		list.add("by #AI and #MachineLearning empower, not replace, government #cybersecurity");
		
//		String test = getTweetText(list);
//		System.err.println(test);
	}
	
	//Function to load files from directory
    public List<String> getFileNames(String path) throws IOException{
    	List<String> fileNames =  new ArrayList<String>();
    	
    	try {
    		
    		 File folder = new File(path);
    	    	File[] listOfFiles = folder.listFiles();
    	    	
    	    	for (int i = 0; i < listOfFiles.length; i++) {
    	    		
    	    	      if (listOfFiles[i].isFile()) {
//    	    	    	  System.out.println("File " + listOfFiles[i].getName());
    	    	    	  fileNames.add(listOfFiles[i].getName());
    	    	    	
    	    	      } else if (listOfFiles[i].isDirectory()) {
//    	    	        System.out.println("Directory " + listOfFiles[i].getName());
    	    	      }
    	    	    }
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
       
    	
    	return fileNames;
    }

	
	 public String[] getArrayIds(ArrayList<String> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
    
  //Function to clean files from directory
    public List<String> cleanDirectory(String path) throws IOException{
    	List<String> fileNames =  new ArrayList<String>();
        	File folder = new File(path);
    	File[] listOfFiles = folder.listFiles();
    	
    	for (int i = 0; i < listOfFiles.length; i++) {
    		
    	      if (listOfFiles[i].isFile()) {
    	    	  
    	    	  fileNames.add(listOfFiles[i].getName());
    	    	  listOfFiles[i].delete();
//    	    	  System.out.println("File " + listOfFiles[i].getName());	  
    	    	
    	      } else if (listOfFiles[i].isDirectory()) {
//    	        System.out.println("Directory " + listOfFiles[i].getName());
    	      }
    	    }
    	
    	return fileNames;
    }
    
    public LinkedList<ArrayList<Map<String, Object>>> getDataInBulks(ArrayList<Map<String, Object>> StatusORprofilesData,int bulkSize){
    	
    	 LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
			for (int i = 0; i < StatusORprofilesData.size(); i += bulkSize) {
				ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(
						StatusORprofilesData.subList(i,
								Math.min(i + bulkSize, StatusORprofilesData.size())));
				bulks.add(bulk);
			}
			return bulks;
    }
    
    //function to make multi line string from list 
    public String  getTweetText(List<String> tweetText) {
    	
    	StringBuilder buildText  = new StringBuilder();
    	for (int i =0;i< tweetText.size(); i++) {
    		if (i==0) {
    			
    			buildText.append(tweetText.get(i).split("-=")[1]+"\n");
			}
    		else {
    			buildText.append(tweetText.get(i)+"\n");
			}
    		
		}
    	
    	String tweet = buildText.toString();
	
    return tweet;
    }
    
    
 // Function to LOAD FILE DATA
   	public List<String> loadFile(String file)	throws IOException {
   		List<String> searchJobs = new ArrayList<String>();
   		
   		try {
   		// CSVReader reader1 = new CSVReader(new FileReader("yourfile.csv"));
   	   		InputStream in = new FileInputStream(new File(file));
   	   		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
   	   		String line="";
   	  		while ((line = reader.readLine()) != null) {
   	  			searchJobs.add(line);
   	  	}
   	  		reader.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
	}
   		
  	return searchJobs;
   	}
   	
   	public List<String> pickNRandom(List<String> lst, int n) {
   	    List<String> copy = new LinkedList<String>(lst);
   	    try {
   	    	
   	    	Collections.shuffle(copy);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
   	    return copy.subList(0, n);
   	    
   	}
   	
 // Function to LOAD FILE DATA
   	public ArrayList<Long> loadFileIds(String file)	throws IOException {
   		ArrayList<Long> searchJobs = new ArrayList<Long>();
   		ObjectMapper mapper = new ObjectMapper();
   		TypeReference<ArrayList<Long>> typeRef = new TypeReference<ArrayList<Long>>() {	};
   		// CSVReader reader1 = new CSVReader(new FileReader("yourfile.csv"));
   		InputStream in = new FileInputStream(new File(file));
   		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
   		String line="";
  		while ((line = reader.readLine()) != null) {
  			
  			searchJobs = mapper.readValue(line, typeRef);
  	}
  		reader.close();
  		
  		return searchJobs;
   	}
   	
	// Function to LOAD ArrayList of Map  DATA
	public LinkedList<ArrayList<Map<String, Object>>> loadProfilesData(String file) throws IOException {
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		LinkedList<ArrayList<Map<String, Object>>> profilesBulks = new LinkedList<ArrayList<Map<String, Object>>>();
		ObjectMapper mapper = new ObjectMapper();
		TypeReference<ArrayList<Map<String, Object>>> typeRef = new TypeReference<ArrayList<Map<String, Object>>>() {	};
		
		InputStream in = new FileInputStream(new File(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		
		while ((line = reader.readLine()) != null) {

		 tweets = mapper.readValue(line, typeRef);
		 profilesBulks.add(tweets);
		}
		reader.close();

		return profilesBulks;
	}
	
	// Function to LOAD ArrayList of Map  DATA
		public ArrayList<Map<String, Object>> loadProfilesDataTweets(String file) throws IOException {
			ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<ArrayList<Map<String, Object>>> typeRef = new TypeReference<ArrayList<Map<String, Object>>>() {	};

			InputStream in = new FileInputStream(new File(file));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			
			while ((line = reader.readLine()) != null) {

			 tweets = mapper.readValue(line, typeRef);
			}
			reader.close();

			return tweets;
		}
   	
   	public String listToString(List<String> terms){
		String keyword = "";
		int count =0;
		try {
			
			for (int i = 0; i < terms.size(); i++) {
				
				if ( count == terms.size()-1 ) {

					keyword = keyword +	"(\""+terms.get(i)+"\")";
					}
				else {
					keyword = keyword +	"(\""+terms.get(i)+"\") OR ";
				}
		
				count++;
		
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		 
	
		return keyword;
	}
   	
   	public long[] getArrayListAslong(ArrayList<String> ids){
   		
		Collections.sort(ids.subList(0, ids.size()));
		  long [] id =new long[ids.size()];
		  try {
	   			for (int i=0;i< ids.size();i++) {
	  			  
	   				id[i] =Long.parseLong(ids.get(i).toString());
	   			}
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
	  
		  return id;
	  }
   	
   	public LinkedList<long[]> chunks(long[] bigList, int n) {
   		LinkedList<long[]> partitions = new LinkedList<long[]>();
   		try {
   			int partitionSize = n;
   			
   			for (int i = 0; i < bigList.length; i += partitionSize) {
   				long[] bulk = Arrays.copyOfRange(bigList, i,
   						Math.min(i + partitionSize, bigList.length));
   				partitions.add(bulk);
   			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		

		return partitions;
	}
   	
 	public ArrayList<String> joinList(LinkedList<ArrayList<String>> listParts) {
		
		ArrayList<String> joinedList = new ArrayList<String>();
		for (ArrayList<String> part : listParts) {
	for (int i = 0; i < part.size(); i++) {
		joinedList.add(part.get(i));
	}
		}

		return joinedList;
	}
 	
 	public long getRandomeValue(long min, long max){
 		
 	 long LOWER_RANGE =min; //assign lower range value
   	 long UPPER_RANGE = max; //assign upper range value
   	 Random random = new Random();
   	 long randomValue = LOWER_RANGE +(long)(random.nextDouble()*(UPPER_RANGE - LOWER_RANGE));
   	 
   	 return randomValue;
   	 
 	}
 	
}
