package org.xululabs.commands;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8.Fun;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "retweet", commandDescription = "use to retweet tweets")
public class CommandRetweet extends BaseCommand {

	int bulkSize = 500;
	private static Logger log = LogManager.getRootLogger();
	private MysqlApi mysql = new MysqlApi();
	private UtilFunctions UtilFunctions = new UtilFunctions();
	Twitter4jApi twittertApi = new Twitter4jApi();
	@Parameter(names = "-dbName", description = "get dbName", required = true)
	private String dbName;
	@Parameter(names = "-dbUser", description = "get dbUser", required = true)
	private String dbUser;
	@Parameter(names = "-dbPassword", description = "get dbPass", required = true)
	private String dbPassword;
	@Parameter(names = "-id", description = "get directory of files", required = true)
	private String inputFilepath;
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
		return inputFilepath;
	}

	public void setFilePath(String filepath) {
		this.inputFilepath = filepath;
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
		Twitter twitter = null;
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));

		long min = 60000;
		long max = 1200000;
		
		long sleepTime = UtilFunctions.getRandomeValue(min, max);
		
		long minutes = TimeUnit.MILLISECONDS.toMinutes(sleepTime);
		
		System.err.println("sleeping for "+minutes +" minutes , milliseconds :"+sleepTime );
		
		Thread.sleep(sleepTime);
		
		System.err.println("wokeUp from Sleep ...!");
		
		//getting fileNames in a given directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
		
		for (String fileName : fileNames) {

			String uId_gid =fileName.split("_")[0]+"_"+fileName.split("_")[1];
			System.err.println("processing user: "+uId_gid);
			File file1 = new File(this.getFilePath()+"/"+uId_gid+"_retweetIds");

			if (!(file1.exists())) {
				System.err.println(file1);
				System.err.println("skipping "+uId_gid+" either missing data file");
				continue;
			}
			
			ArrayList<String> tweetIds = (ArrayList<String>) UtilFunctions.loadFile(this.getFilePath()+"/"+uId_gid+"_retweetIds");
			
			System.err.println(uId_gid +" status ids:  "+tweetIds);
			
			ArrayList<String> retweetedIds = new ArrayList<String>();
			
			List<String> allUids = mysql.getAllUidsGids(this.getDbName(),this.getDbUser(),this.getDbPassword());
			
			String stringUid =fileName.split("_")[0]+"_"+fileName.split("_")[1];
			
			int bit =0;
			bit = allUids.contains(stringUid)? 1 : 0;
			
			if (bit==0) {
				System.err.println(stringUid+" missing from tt_twitter_app");
				continue;
			}
			
			Map<String, Object> consumerKey = mysql.getAuthKeysByUid(this.getDbName(),this.getDbUser(),this.getDbPassword(), uId_gid);
			twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
			
			int index = 0;
			while (twitter.getRetweetsOfMe().getRateLimitStatus().getLimit() > 0 && index < tweetIds.size()) {
			try {
				long retweetId = Long.parseLong(tweetIds.get(index));
				
			Status retweetStatus = twitter.showStatus(retweetId);
			
			if (!retweetStatus.isRetweetedByMe()) {
				
				Status retweet = twitter.retweetStatus(retweetId);
				retweetedIds.add(tweetIds.get(index).toString());
				
			}
				
			} catch (TwitterException ex) {
				
				System.err.println(ex.getMessage());
				
				if (ex.getErrorCode()==88) {
					
					System.err.println("Auyth keys limit exceeded : ");
				}
				else {
					
					retweetedIds.add(tweetIds.get(index).toString());

				}
				
				System.err.println("issue for the user "+uId_gid);
				index++;
//				continue;
			}
			index++;
		}
			
				ttafResponse = new TtafResponse(retweetedIds);
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + uId_gid+ "_retweetedIds")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				System.err.println(" retweets Done for "+ uId_gid+" ..! "+retweetedIds);
				ttafResponse = null;
		}
		
		System.err.println(" retweets Done for All ..!");
		
		return ttafResponse;
	}

	@Override
	public void write(TtafResponse ttafResponse, BufferedWriter writer)
			throws Exception {
		ArrayList<String> relationIds = (ArrayList<String>) ttafResponse.getResponseData();
	      
        String jsonSettings = new Gson().toJson(relationIds);
        writer.append(jsonSettings);
        writer.newLine();
		
			
	}

}