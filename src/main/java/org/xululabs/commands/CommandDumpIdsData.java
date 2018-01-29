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

import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "dumpProfilesData", commandDescription = "Dump IDs data from twitter")
public class CommandDumpIdsData extends BaseCommand {
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

	MysqlApi mysql = new MysqlApi();
	UtilFunctions UtilFunctions = new UtilFunctions();
	long userId = -1;

	@Override
	public TtafResponse execute() throws TwitterException, IOException,
			ClassNotFoundException, SQLException, InterruptedException {

		Map<String, Object> consumerKey = new HashMap<String, Object>();
		ObjectMapper mapper = new ObjectMapper();
		int deadCondition = 0;
		BufferedWriter bufferedWriter = null;
		Twitter twitter = null;
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		LinkedList<Map<String, Object>> profilesData = new LinkedList<Map<String,Object>>();
		TypeReference<HashMap<String,List<String>>> typeRefForRelationIds = new TypeReference<HashMap<String,List<String>>>() {	};
	
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		TtafResponse ttafResponse = null;
		Set<String> screenNames = new HashSet<String>();
				
		// Function to read files in directory getting all files in directory
		List<String> fileNames =  UtilFunctions.getFileNames(this.getFilePath());
		
		// making all screenNames unique to avoid from duplication
		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("=");
			screenNames.add(fileParts[0]);
		}
		
		// getting Authentication keys from Database
					consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
					System.err.println(consumerKey.get("consumerKey").toString());
					//updating database  table for timeStamp so it can be used after all others are consumed
					mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
					
					twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
							.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());

		
		for (String screenName : screenNames) {
			
			
			String relationkey=""; 
			boolean enterDead = false;
			String pathRelationIds = this.getFilePath()+"/"+screenName+"=relationIds";
			List<String> loadRelationIds = UtilFunctions.loadFile(pathRelationIds);
			Map<String, ArrayList<String>> relationIds = mapper.readValue(loadRelationIds.get(0), typeRefForRelationIds);
			for (String key : relationIds.keySet()) {
			relationkey = key;
			bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + screenName+ "="+relationkey)));
			
			long relationList [] = UtilFunctions.getArrayListAslong(relationIds.get(key));		
			int remApiLimits = 900;
			int apiremaining = 180;
			int profilesDataSize = 0;
				
	
			LinkedList<long[]> chunks = UtilFunctions.chunks(relationList, 100);

					for (int j = 0; j < chunks.size(); j++) {
			try {
				
					if (enterDead) {
						
						
							consumerKey.clear();
							consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
		    				mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
		    				System.err.println(consumerKey.get("consumerKey").toString());
		    				twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
		    				enterDead = false;
	
						}
					
					Map<String, RateLimitStatus> rateLimitStatusAppi = twitter.getRateLimitStatus("application");
    				RateLimitStatus	AppiRateLimit = rateLimitStatusAppi.get("/application/rate_limit_status");
    				apiremaining = AppiRateLimit.getRemaining();
						if (remApiLimits < 10 || 	apiremaining < 20) {
							
							if (apiremaining < 18 ) {
								
								System.err.println("limits exceeding going to sleep ..!");
								Thread.sleep(900000);
								System.err.println("wokeUp from sleep ..!");
								
							}
							
							System.err.println("changing auth kesy limit "+remApiLimits +"api remaining "+apiremaining);
	    					System.err.println("position "+j);
	    					twitter = null;
	    					enterDead =true;
	    					j--;
							deadCondition = 0;
							continue;
						}
						
//						System.err.println(remApiLimits +" api"+ apiremaining);
						ResponseList<User> users = twitter.lookupUsers(chunks.get(j));
						users.getRateLimitStatus().getRemaining();
						Map<String, Object> tweetInfo = null;
						for (int i = 0; i < users.size();i++) {	
							
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
							profilesData.add(tweetInfo);
						}
					
					remApiLimits = users.getRateLimitStatus().getRemaining();
					
					
					if (profilesData.size() == 500 || profilesData.size() > 500 || ( (j == chunks.size()-1) && profilesData.size() < 500)) {
						profilesDataSize = profilesDataSize+profilesData.size();
						ttafResponse = new TtafResponse(profilesData);
						write(ttafResponse, bufferedWriter);
						profilesData.clear();
					}
					
					}
					catch (TwitterException e) {
						
						System.err.println(e.getMessage());
						
						deadCondition++;
						if (e.getErrorCode() == 88) {
							
							System.err.println("deadlock condition getting new auth keys ");
											
							if (deadCondition == 4) {
								
							Thread.sleep(800000);
							deadCondition = 0;
						}
							j--;
							continue;
							
				}
						
						if (e.getErrorCode() ==32) {
							System.err.println("issue in the keys :"+consumerKey);
						}
							
					}
				} 
				
				
				bufferedWriter.close();
				System.err.println("profiles Data for " + screenName + " Done..! size "+profilesDataSize);
//				profilesData.clear();
			}	
				
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
	/**
	 * use to get twitter instance
	 * 
	 * @param consumerKey
	 * @param consumerSecret
	 * @param accessToken
	 * @param accessTokenSecret
	 * @return twitter
	 */

	public Twitter getTwitterInstance(String consumerKey,
			String consumerSecret, String accessToken, String accessTokenSecret) {
		Twitter twitter = null;
		try {
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
					.setOAuthConsumerSecret(consumerSecret)
					.setOAuthAccessToken(accessToken)
					.setOAuthAccessTokenSecret(accessTokenSecret);
			TwitterFactory tf = new TwitterFactory(cb.build());
			twitter = tf.getInstance();
		} catch (Exception e) {

			System.err.println(e.getMessage());

		}

		return twitter;
	}
}
