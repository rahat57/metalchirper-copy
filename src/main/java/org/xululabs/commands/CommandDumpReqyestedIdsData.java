package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "dumpRequestedProfilesData", commandDescription = "Friends IDs")
public class CommandDumpReqyestedIdsData extends BaseCommand {
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

		Map<String, Object> consumerKey = null;
		ObjectMapper mapper = new ObjectMapper();
		TypeReference<ArrayList<String>> typeRefForRelationIds = new TypeReference<ArrayList<String>>() {	};
		Twitter twitter = null;
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		TtafResponse ttafResponse = null;
		int deadCondition = 0;
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		LinkedList<Map<String, Object>> profilesData = new LinkedList<Map<String,Object>>();
		
		// Function to read files in directory getting all files in directory
		List<String> fileNames =  UtilFunctions.getFileNames(this.getFilePath());

		//iteration over each screenNames and reading its ids and dumping dumping profiles from twitter
		for (String fileName : fileNames) {

			String relationkey=""; 
			
			
			String pathRequestedIds = this.getFilePath()+"/"+fileName;
			List<String> loadRequestedIds = UtilFunctions.loadFile(pathRequestedIds);
			
			ArrayList<String> requestedIds = mapper.readValue(loadRequestedIds.get(0), typeRefForRelationIds);
			relationkey = fileName+"ProfilesData";
	
			long relationList [] = UtilFunctions.getArrayListAslong(requestedIds);	

			int remApiLimits = 900;
			
		try {
			
				LinkedList<long[]> chunks = UtilFunctions.chunks(relationList, 100);
				
				// getting Authentication keys from Database
				consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
				
				//updating database  table for timeStamp so it can be used after all others are consumed
				mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
				
				twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
							.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
					
						
					for (int j = 0; j < chunks.size();j++) {
						
						//if limit exceeding changing keys
						if (remApiLimits < 12) {
							
							consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
							
							mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
							
							twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
									.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
							deadCondition = 0;	
						}
						
						ResponseList<User> users = twitter.lookupUsers(chunks.get(j));
						users.getRateLimitStatus().getRemaining();
						Map<String, Object> tweetInfo = null;
						for (int i = 0; i < users.size(); i++) {	
							
							tweetInfo = new HashMap<String, Object>();
							tweetInfo.put("id", users.get(i).getId());
							tweetInfo.put("screenName", users.get(i).getScreenName());
							tweetInfo.put("tweetsCount", users.get(i).getStatusesCount());
							tweetInfo.put("followersCount", users.get(i).getFollowersCount());
							tweetInfo.put("friendsCount", users.get(i).getFriendsCount());
							double friendsCount = users.get(i).getFriendsCount();
						    double followersCounts = users.get(i).getFollowersCount();
						    double ratio = 0;
								if (friendsCount!=0) {
									 ratio = (followersCounts/friendsCount);
								}
								
							tweetInfo.put("ratio", ratio);
							tweetInfo.put("user_image", users.get(i).getProfileImageURL());
							tweetInfo.put("description", users.get(i).getDescription());
							tweetInfo.put("user_location", users.get(i).getLocation());
							tweetInfo.put("date",dateFormat.format(date));
							tweetInfo.put("timeZone",users.get(i).getUtcOffset());
							tweetInfo.put("time",users.get(i).getCreatedAt());
							profilesData.add(tweetInfo);
						}
					
					remApiLimits = users.getRateLimitStatus().getRemaining();
					
					}
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
				ttafResponse = new TtafResponse(profilesData);
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" +relationkey)));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				System.err.println("profiles Data for " + fileName + " Done..! size "+profilesData.size());
				profilesData.clear();
				
				
		}
		System.out.println("dumping profiles Data for All Done ..!");
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
