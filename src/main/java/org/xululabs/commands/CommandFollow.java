package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "follow", commandDescription = "follow user on twitter")
public class CommandFollow extends BaseCommand {

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
		Set<String> uIds = new HashSet<String>();
		List<String> followedScreenNames = new ArrayList<String>();
		BufferedWriter bufferedWriter = null;
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		
		long min = 60000; //min 1 minute
		long max = 540000; // max 9 minutes
		
		long sleepTime = UtilFunctions.getRandomeValue(min, max);
		
		long minutes = TimeUnit.MILLISECONDS.toMinutes(sleepTime);
		
		System.err.println("sleeping for "+minutes +" minutes , milliseconds :"+sleepTime );
		
		Thread.sleep(sleepTime);
		
		System.err.println("wokeUp from Sleep ...!");
		
		// Function to read files in directory getting all files in directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
		
		System.err.println("files in directory: "+fileNames);
		
		// making all user Ids unique to avoid from duplication
		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("_");
			uIds.add(fileParts[0]+"_"+fileParts[1]);
		}
		
		//iteration over each user id and perform follow for each
		for (String uid : uIds) {
			
			//checking if file exist
			File file1 = new File(this.getFilePath()+"/"+uid+"_followScreenNames");
			
			if (!(file1.exists())) {
				System.err.println("skipping "+uid+" either missing data file");
				continue;
			}
			
			//loading screenNames to perform Follow operation 
			List<String> followScreenNames = UtilFunctions.loadFile(this.getFilePath()+"/"+uid+"_followScreenNames");
			
			System.err.println(uid +" follow screen names: "+followScreenNames);
			
			// checking if uid exist in db table
			List<String> allUids = mysql.getAllUidsGids(this.getDbName(),this.getDbUser(),this.getDbPassword());
			
			int bit = 0;
			bit = allUids.contains(uid)? 1 : 0;
			
			if (bit==0) {
				System.err.println(uid+" missing from tt_twitter_app");
				continue;
			}
			
			
			// getting Authentication keys for specific user
			Map<String, Object> consumerKey = mysql.getAuthKeysByUid(this.getDbName(),this.getDbUser(),this.getDbPassword(), uid);
			twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
			int index = 0;
			
				
				for (int i = 0; i < followScreenNames.size(); i++) {
					
					try {
						
					User followedUser = twitter.createFriendship(followScreenNames.get(index));
					
					if (!followedUser.getScreenName().isEmpty()) {
						followedScreenNames.add(followedUser.getScreenName());
						index++;
						}
					
				}
				
					catch (TwitterException ex) {
						
						System.err.println(ex.getMessage());
						
						if (ex.getErrorCode()==88) {
							
							System.err.println("Auyth keys limit exceeded : ");
						}
						
						else {
							
							followedScreenNames.add(followScreenNames.get(index));
						}
					
						if (ex.getErrorCode() ==32) {
							System.err.println("issue in the keys :"+consumerKey);
						}
						
						System.err.println("issue for the user "+uid);
						index++;
					}

				} 
			
				ttafResponse = new TtafResponse(followedScreenNames);
				bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + uid+ "_followedScreenNames")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				
				System.err.println(" follow Done for "+ uid+" ..! "+followedScreenNames);
				ttafResponse = null;
				followedScreenNames.clear();
		}
		
		System.err.println(" follow Done for All ..!");
		 return ttafResponse;
	}

	@Override
	public void write(TtafResponse ttafResponse, BufferedWriter writer)
			throws Exception {
		List<String> relationIds = (ArrayList<String>) ttafResponse.getResponseData();
	      
        String jsonSettings = new Gson().toJson(relationIds);
        writer.append(jsonSettings);
        writer.newLine();
		
			
	}
	
}