package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.URLEntity;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "dumpUserTimeLine", commandDescription = "Friends IDs")
public class CommandDumpUserTimeLine extends BaseCommand {
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

	@Override
	public TtafResponse execute() throws TwitterException, IOException,
			ClassNotFoundException, SQLException, InterruptedException {

		Map<String, Object> consumerKey = new HashMap<String,Object>();
		LinkedList<Map<String, Object>> userTimeLine = new LinkedList<Map<String,Object>>();
		Twitter twitter = null;
		TtafResponse ttafResponse = null;
		int deadCondition = 0;
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		
		// Function to read files in directory getting all files in directory
		List<String> fileNames =  UtilFunctions.getFileNames(this.getFilePath());
		Set<String> allScreenNames = Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(16, 0.75f, true));

		//parsing filenames and keeping screenNames unique
		for (int i = 0; i < fileNames.size(); i++) {
			
			List<String> screenNames = UtilFunctions.loadFile(this.getFilePath() + "/"+ fileNames.get(i));
			               
			for (String screenName : screenNames) {

				allScreenNames.add(screenName);

			}	
		}	

		//iteration over each screenName and getting their timelines
		for (String screenName : allScreenNames) {
			
			int limit = Integer.parseInt(screenName.split(" ")[1]); 
			String outputFileName=screenName.split(" ")[0]+"=timeLine"; 
			
			// getting Authentication keys form database
			consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
			
			mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
			
			twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
					.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
			
			List ListTweetes = new ArrayList();
			int remApiLimits = 900;
			int pageNo = 1 ;
			do {
			
				try {

					if (remApiLimits < 2) {
						
						consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
						
						mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
						
						twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
						deadCondition = 0;	
					}
					
					Paging paging = new Paging(pageNo++, 200);
					int size = ListTweetes.size(); 
					ListTweetes.addAll(twitter.getUserTimeline(screenName.split(" ")[0],paging));

					List<Status> userTimeline = twitter.getUserTimeline(screenName.split(" ")[0],paging);
					
					remApiLimits = twitter.getUserTimeline().getRateLimitStatus().getRemaining();
					
					for (Status tweet: userTimeline) {
						Map<String, Object> tweetInfo = new HashMap<String, Object>();
						tweetInfo.put("id", tweet.getId());
    					tweetInfo.put("tweet", tweet.getText());
    					tweetInfo.put("screenName", tweet.getUser().getScreenName());
    					tweetInfo.put("userId", tweet.getUser().getId());
    					tweetInfo.put("name", tweet.getUser().getName());
    					tweetInfo.put("retweetCount", tweet.getRetweetCount());
    					double friendsCount = tweet.getUser().getFriendsCount();
    					double followersCount = tweet.getUser().getFollowersCount();
    					double ratio = 0;
    					if (friendsCount!=0) {
    						 ratio = (followersCount/friendsCount);
    					}
    					tweetInfo.put("ratio", ratio);
    					tweetInfo.put("followersCount", tweet.getUser().getFollowersCount());
    					tweetInfo.put("friendsCount", tweet.getUser().getFriendsCount());
    					tweetInfo.put("user_image", tweet.getUser().getProfileImageURL());
    					tweetInfo.put("description", tweet.getUser().getDescription());
    					tweetInfo.put("user_location", tweet.getUser().getLocation());
    					tweetInfo.put("tweet_location", tweet.getGeoLocation());
    					tweetInfo.put("time",tweet.getCreatedAt().getTime());
    					String expandedUrl = "";
    					List<String> urls = new ArrayList<String>();
    					for (URLEntity urle : tweet.getURLEntities()) {
    						 expandedUrl = urle.getExpandedURL();
    						 urls.add(urle.getExpandedURL());
    						break;
                    	 	} 
    						tweetInfo.put("externalUrl",expandedUrl);
    						tweetInfo.put("url",urls);

    					tweetInfo.put("date",new SimpleDateFormat("yyyy-MM-dd").format(tweet.getCreatedAt()).toString());
    					tweetInfo.put("timeZone", tweet.getUser().getUtcOffset());
    					userTimeLine.add(tweetInfo);
					}
	
					if (userTimeLine.size() > limit || ListTweetes.size() == size) {
						break;
					}
				}
		
					
				 catch (TwitterException e) {
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
			}
		
			while(true);
		
				ttafResponse = new TtafResponse(userTimeLine);
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" +outputFileName)));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				System.err.println("timeLine for " + screenName.split(" ")[0] + " Done..! size "+userTimeLine.size());
				userTimeLine.clear();			
		}
		
		System.out.println("dumping userTimeLine for All Done ..!");
		ttafResponse = null;
		return ttafResponse;
	}


	@SuppressWarnings("unchecked")
	@Override
	public void write(TtafResponse tsakResponse, BufferedWriter writer)
			throws IOException {
		LinkedList<Map<String,Object>> profilesData = (LinkedList<Map<String,Object>>) tsakResponse.getResponseData();

			String jsonSettings = new Gson().toJson(profilesData);
			writer.append(jsonSettings);
			writer.newLine();
		
	}

}
