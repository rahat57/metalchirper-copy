package org.xululabs.commands;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "indexUsersElasticsearch", commandDescription = "index tweets into elastic search")
public class CommandIndexUsersElasticsearch extends BaseCommand {

	int bulkSize = 500;
	String esHost = "localhost";
	int esPort = 9300;
	private static Logger log = LogManager.getRootLogger();
	MysqlApi mysql = new MysqlApi();
	UtilFunctions UtilFunctions = new UtilFunctions();
	ElasticsearchApi elasticsearch = new ElasticsearchApi();
	@Parameter(names = "-dbName", description = "get dbName", required = true)
	private String dbName;
	@Parameter(names = "-dbUser", description = "get dbUser", required = true)
	private String dbUser;
	@Parameter(names = "-dbPassword", description = "get dbPass", required = true)
	private String dbPassword;
	@Parameter(names = "-id", description = "get filePath to read search Jobs", required = true)
	private String filepath;
	@Parameter(names = "-od", description = "get directory of files", required = true)
	private String ouputFilepath;
	
	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}
	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}
	public String getFilePath() {
		return filepath;
	}

	public void setFilePath(String filepath) {
		this.filepath = filepath;
	}
	
	public String getOuputFilePath() {
		return ouputFilepath;
	}

	public void setOuputFilePath(String ouputFilepath) {
		this.ouputFilepath = ouputFilepath;
	}

	@Override
	public TtafResponse execute() throws Exception {
		TtafResponse ttafResponse = null;
		List<String> userIndexed = new ArrayList<String>();
		Set<String> screenNames = new HashSet<String>(); 
		BufferedWriter bufferedWriter = null;
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		
		//getting fileNames in a given directory
		List<String> fileNames =  UtilFunctions.getFileNames(this.getFilePath());

		//parsing filenames and keeping screenNames unique
		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("=");
			screenNames.add(fileParts[0].trim());
		}
		
		 String indexEncription = "";
		
		// iterate over screenNames to index each user data
		for (String screenName : screenNames) {
			
			List<String> allScreenNames = mysql.getAllScreenNames(this.getDbName(),this.getDbUser(),this.getDbPassword());
			
			
			if (! (allScreenNames.contains(screenName))) {
				
				System.err.println(screenName+" missing from tt_twitter_app");
				continue;
			}
			
			// getting indexEncription from database
			Map<String, Object> indexEncriptionAndUis =  mysql.getUidAndIndexEncryption(this.getDbName(),this.getDbUser(),this.getDbPassword(), screenName);
			indexEncription = indexEncriptionAndUis.get("encrypted").toString();
			
			System.out.println("user index Encryption "+indexEncription);
			try {
				
				// making path with complete filename required to index in elasticsearch
				String pathCommonProfilesData = this.getFilePath()+"/"+screenName+"=commonRelation";
				String pathNonCommonFriendsProfilesData = this.getFilePath()+"/"+screenName+"=nonCommonFriends";
				String pathNonCommonFollowersProfilesData = this.getFilePath()+"/"+screenName+"=nonCommonFollowers";
				
				LinkedList<ArrayList<Map<String, Object>>> profilesBulks = new LinkedList<ArrayList<Map<String, Object>>>();
				File file1 = new File(pathCommonProfilesData);
				File file2 = new File(pathNonCommonFriendsProfilesData);
				File file3 = new File(pathNonCommonFollowersProfilesData);
				ObjectMapper mapper = new ObjectMapper();
				TypeReference<ArrayList<Map<String, Object>>> typeRef = new TypeReference<ArrayList<Map<String, Object>>>() {	};
				// checking if files exist then loading to perform operation
				if (file1.exists()) {
					//reading file from directory and indexing into elasticsearch
					ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
					InputStream in = new FileInputStream(new File(pathCommonProfilesData));
					BufferedReader reader = new BufferedReader(new InputStreamReader(in));
					String line;
					
					while ((line = reader.readLine()) != null) {

					 tweets = mapper.readValue(line, typeRef);
					 this.indexInESearch(tweets,indexEncription,"commonrelation");
					}
					reader.close();
				}
				

				if (file2.exists()) {
					
					//reading file from directory and indexing into elasticsearch
					ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
					InputStream in = new FileInputStream(new File(pathNonCommonFriendsProfilesData));
					BufferedReader reader = new BufferedReader(new InputStreamReader(in));
					String line;
					
					while ((line = reader.readLine()) != null) {

					 tweets = mapper.readValue(line, typeRef);
					 this.indexInESearch(tweets,indexEncription,"noncommonfriends");
					}
					reader.close();
				}
				
				
				if (file3.exists()) {
					
					//reading file from directory and indexing into elasticsearch
					ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
					InputStream in = new FileInputStream(new File(pathNonCommonFollowersProfilesData));
					BufferedReader reader = new BufferedReader(new InputStreamReader(in));
					String line;
					
					while ((line = reader.readLine()) != null) {

					 tweets = mapper.readValue(line, typeRef);
					 this.indexInESearch(tweets,indexEncription,"noncommonfollowers");
					}
					reader.close();
					
				}


		}  catch (Exception e) {
			
			System.err.println(e.getMessage());
			
			}
			
			userIndexed.add(screenName);
			System.out.println("indexing done for user "+screenName);

		}
		
		System.err.println("users "+userIndexed);
		ttafResponse = new TtafResponse(userIndexed);
		bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" +"usersIndexed")));
		write(ttafResponse, bufferedWriter);
		bufferedWriter.close();
		ttafResponse = null;
		return ttafResponse;
	}

	@Override
	public void write(TtafResponse ttafResponse, BufferedWriter writer)
			throws Exception {
		
		List<String> indexedUsers = (ArrayList<String>) ttafResponse.getResponseData();
        String jsonSettings = new Gson().toJson(indexedUsers);
        writer.append(jsonSettings);
        writer.newLine();
		System.out.println("indexing Done for All ..!");
			
	}
	
	/**
	 * use to index tweets in ES
	 * 
	 * @param tweets
 * @throws Exception 
	 */
		public void indexInESearch(ArrayList<Map<String, Object>> tweets,String indexName,String type)throws Exception {
				int count =1;
			TransportClient client = this.elasticsearch.getESInstance(this.esHost,this.esPort);
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			for (Map<String, Object> tweet : tweets) {

					bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));	
				}				
			bulkRequestBuilder.setRefresh(true).execute().actionGet();
			
			client.close();	
	
	}

}