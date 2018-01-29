package org.xululabs.commands;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.mysql.cj.api.log.Log;

@Parameters(commandNames = "indexTweetsElasticsearch", commandDescription = "index tweets into elastic search")
public class CommandIndexTweetsElasticsearch extends BaseCommand {

	int bulkSize = 500;
	private String esHost = "localhost";
	private int esPort = 9300;
	private static Logger log = LogManager.getRootLogger();
	MysqlApi mysql = new MysqlApi();
	UtilFunctions UtilFunctions = new UtilFunctions();
	@Parameter(names = "-dbName", description = "get dbName", required = true)
	private String dbName;
	@Parameter(names = "-dbUser", description = "get dbUser", required = true)
	private String dbUser;
	@Parameter(names = "-dbPassword", description = "get dbPass", required = true)
	private String dbPassword;
	@Parameter(names = "-id", description = "get filePath to read search Jobs", required = true)
	private String filepath;
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

	@Override
	public TtafResponse execute() throws Exception {
		TtafResponse ttafResponse = null;
		
		//getting fileNames in a given directory
		List<String> fileNames =  UtilFunctions.getFileNames(this.getFilePath());
		
		//iterating over each and index data into elasticsearch
		for (int f = 0; f <fileNames.size(); f++) {
			
			String fileParts[] = fileNames.get(f).split("_");
			String uId_gid = fileParts[0]+"_"+fileParts[1];	
			String indexEncription = "tweets_"+uId_gid;
		
		ArrayList<Map<String, Object>> tweets = UtilFunctions.loadProfilesDataTweets(this.getFilePath()+"/"+fileNames.get(f));  

		
		String type = "tweets";
			try {				


			 LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();	
			 bulks = UtilFunctions.getDataInBulks(tweets, this.bulkSize);
				
				for (ArrayList<Map<String, Object>> tweetsList : bulks) {
					this.indexInES(indexEncription,type,tweetsList);
					
				}
				
				bulks.clear();
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
			
		} catch (SQLException e) {
			
			e.printStackTrace();
			
		}
		catch (IOException e) {
			
			e.printStackTrace(); 
		}
		 ttafResponse = new TtafResponse(tweets);
		  System.err.println("indexing done for user "+indexEncription);
		}
		 return ttafResponse;
	}

	@Override
	public void write(TtafResponse ttafResponse, BufferedWriter writer)
			throws Exception {
		System.err.println("indexing Done for All ..!");
			
	}


	/**
	 * use to index tweets in ES
	 * 
	 * @param tweets
	 * @throws Exception
	 */
	public void indexInES(String indexName, String type,
			ArrayList<Map<String, Object>> tweets) throws Exception {
		ElasticsearchApi elasticsearch = new ElasticsearchApi();
		TransportClient client = elasticsearch.getESInstance(this.esHost,this.esPort);
		
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Map<String, Object> tweet : tweets) {

			bulkRequestBuilder.add(client
					.prepareUpdate(indexName, type, tweet.get("id").toString())
					.setDoc(tweet).setUpsert(tweet));

		}
		
		bulkRequestBuilder.setRefresh(true).execute().actionGet();

		client.close();
	}

}