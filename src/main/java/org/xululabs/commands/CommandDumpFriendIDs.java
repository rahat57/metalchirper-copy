package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "dumpFriendIDs", commandDescription = "Friends IDs")

public class CommandDumpFriendIDs extends BaseCommand {
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

	private static Logger log = LogManager.getRootLogger();
	MysqlApi mysql = new MysqlApi();
	UtilFunctions UtilFunctions = new UtilFunctions();
	Twitter4jApi twittertApi = new Twitter4jApi();
	long userId = -1;

	@Override
	public TtafResponse execute() throws TwitterException, IOException,
			ClassNotFoundException, SQLException, InterruptedException {

		Map<String, Object> consumerKey = new HashMap<String, Object>();
		Twitter twitter = null;
		TtafResponse ttafResponse = null;
		BufferedWriter bufferedWriter = null;
		int deadCondition = 0;
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		
		// Function to read files in directory getting all files in directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());

		Set<String> allScreenNames = new HashSet<String>();
		
		// making all screenNames unique to avoid from duplication
		for (int i = 0; i < fileNames.size(); i++) {
			
			List<String> screenNames = UtilFunctions.loadFile(this.getFilePath() + "/"+ fileNames.get(i));
			for (String screenName : screenNames) {
				allScreenNames.add(screenName);
			}
			
		}
		
		//iteration over each screenNames and dumping Follower Ids for each
		for (String screenName : allScreenNames) {

			System.err.println("processing user: "+screenName+" dumping Friend Ids");
			
			// getting Auth keys from Database
			consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
			
		   //updating database  table for timeStamp so it can be used after all others are consumed
			mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
			
			twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
					.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
		
			ArrayList<String> friendsIds = new ArrayList<String>();
			int remApiLimits = 15;
			long cursor = -1;
			IDs friendIDs = null;
			long[] friendIds = null;
			do {
			
				try {

					if (remApiLimits < 3) {
						
						if (remApiLimits < 2 ) {
							System.err.println("limits exceeding going to sleep ..!");
							Thread.sleep(900000);
							System.err.println("wokeUp from sleep ..!");
						}
						
						System.err.println("changing Auth keys limit "+remApiLimits+" keys "+consumerKey);
						
						consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
						
						mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
						
						twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
						deadCondition = 0;
					}
					
					friendIDs = twitter.getFriendsIDs(screenName,cursor);
					friendIds = friendIDs.getIDs();
					cursor = friendIDs.getNextCursor();
					remApiLimits = friendIDs.getRateLimitStatus().getRemaining();
					
					for (int i = 0; i < friendIds.length; i++) {
						String id = Long.toString(friendIds[i]);
						friendsIds.add(id);
					}
					
					System.err.println("remaining limit: "+remApiLimits+" Friend Ids dumped "+friendsIds.size());

				} catch (TwitterException e) {
					deadCondition++;
					if (e.getErrorCode() == 88) {
						
						System.err.println("deadlock condition getting new auth keys ");
						
						consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
						
						mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
						
						twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
					if (deadCondition == 4) {
						Thread.sleep(800000);
						deadCondition = 0;
						}
					}
				}

			} while (cursor != 0);

			ttafResponse = new TtafResponse(friendsIds);
			bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + screenName+ "=friendIds")));
			write(ttafResponse, bufferedWriter);
			bufferedWriter.close();
			
			System.err.println("dumping Friend ids for " + screenName + " Done..! size "+friendsIds.size());
		}
		System.out.println("dumping Friend ids for All Done ..!");
		ttafResponse = null;
		return ttafResponse;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(TtafResponse tsakResponse, BufferedWriter writer)
			throws IOException {
		List<String> friendsIds = (List<String>) tsakResponse.getResponseData();

			String jsonSettings = new Gson().toJson(friendsIds);
			writer.append(jsonSettings);
			writer.newLine();
		
	}

}
