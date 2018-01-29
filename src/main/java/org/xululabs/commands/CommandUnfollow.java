package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "unfollow", commandDescription = "unfollow user on twitter")
public class CommandUnfollow extends BaseCommand {

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
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
		Set<String> uIds = new HashSet<String>();
		ObjectMapper mapper = new ObjectMapper();
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));

		long min = 60000; //min 1 minute
		long max = 540000; // max 9 minutes
		
		long sleepTime = UtilFunctions.getRandomeValue(min, max);
		
		long minutes = TimeUnit.MILLISECONDS.toMinutes(sleepTime);
		
		System.err.println("sleeping for "+minutes +" minutes , milliseconds :"+sleepTime );
		
		Thread.sleep(sleepTime);
		
		System.err.println("wokeUp from Sleep ...!");
		
		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("=");
			uIds.add(fileParts[0]);
		}
		
		for (String userId : uIds) {
		
//			System.err.println(screenName);
			
			File file1 = new File(this.getFilePath()+"/"+userId+"=friendsApprovedToUnfollow");
			if (!(file1.exists())) {
				System.err.println("skipping "+userId+" either missing data file");
				continue;
			}
			
			//loading screenNames to perform Follow operation
			List<String> unFollowScreenNames = UtilFunctions.loadFile(this.getFilePath()+"/"+userId+"=friendsApprovedToUnfollow");
			
			List<String> unfollowIdList = new ArrayList<String>();
			
			System.err.println(userId +" Unfollow screen names: "+unFollowScreenNames);
			
			List<String> unFollowedScreenNames = new ArrayList<String>();

			
			// checking if uid exist in db table
						List<String> allUids = mysql.getAllGids(this.getDbName(),this.getDbUser(),this.getDbPassword());
						
						int bit = 0;
						bit = allUids.contains(userId)? 1 : 0;
						
						if (bit==0) {
							System.err.println(userId+" missing from tt_twitter_app");
							continue;
						}
			
			Map<String, Object> consumerKey = mysql.getAuthKeysByUid(this.getDbName(),this.getDbUser(),this.getDbPassword(), userId);
			
			twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
			
			int index = 0;
			
				
				for (String user : unFollowScreenNames) {
					
					try {
					
					User unFollowedUser = twitter.destroyFriendship(unFollowScreenNames.get(index));
					
					if ( unFollowedUser == null ) {
						
						System.err.println("in empty ");
						continue;
					}
					Thread.sleep(10000);
					if (!unFollowedUser.getScreenName().isEmpty()) {
						unFollowedScreenNames.add(unFollowedUser.getScreenName());
						index++;
						}
				}
				
					catch (TwitterException ex) {
						
						if (ex.getErrorCode()==88) {
							
							System.err.println("Auyth keys limit exceeded : ");
						}
						else {
							
							
							unFollowedScreenNames.add(unFollowScreenNames.get(index));
							
						}
						
						System.err.println(ex.getMessage());
						index++;
					}
			} 
			
				ttafResponse = new TtafResponse(unFollowedScreenNames);
				BufferedWriter bufferedWriter = null;
				bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + userId+ "_friendsUnfollowedScreenNames")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				System.err.println(" Unfollow Done for "+ userId+" ..! "+unFollowedScreenNames);
				ttafResponse = null;
		}
		
		System.err.println(" Unfollow Done for All ..!");
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