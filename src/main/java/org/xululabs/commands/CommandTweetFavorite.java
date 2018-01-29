package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;
import org.xululabs.datasources.UtilFunctions;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "likeTweet", commandDescription = "use to like tweets")
public class CommandTweetFavorite extends BaseCommand {

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
		
		//getting fileNames in a given directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
		
		long min = 60000;
		long max = 1200000;
		
		long sleepTime = UtilFunctions.getRandomeValue(min, max);
		
		long minutes = TimeUnit.MILLISECONDS.toMinutes(sleepTime);
		
		System.err.println("sleeping for "+minutes +" minutes , milliseconds :"+sleepTime );
		
		Thread.sleep(sleepTime);
		
		System.err.println("wokeUp from Sleep ...!");
		
		for (String fileName : fileNames) {

			String uid_gid = fileName.split("_")[0]+"_"+fileName.split("_")[1];
			
			File file1 = new File(this.getFilePath()+"/"+uid_gid+"_likeIds");

			if (!(file1.exists())) {
				System.err.println(file1);
				System.err.println("skipping "+uid_gid+" either missing data file");
				continue;
			}
			
			ArrayList<String> tweetIds = (ArrayList<String>) UtilFunctions.loadFile(this.getFilePath()+"/"+uid_gid+"_likeIds");
			
			System.err.println(uid_gid +" status ids:  "+tweetIds);
			
			ArrayList<String> likedIds = new ArrayList<String>();
			
			List<String> allUids = mysql.getAllUidsGids(this.getDbName(),this.getDbUser(),this.getDbPassword());
			
			int bit =0;
			bit = allUids.contains(uid_gid)? 1 : 0;
			
			if (bit==0) {
				System.err.println(uid_gid+" missing from tt_twitter_app");
				continue;
			}
			
			Map<String, Object> consumerKey = mysql.getAuthKeysByUid(this.getDbName(),this.getDbUser(),this.getDbPassword(), uid_gid);
			twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
			int index = 0;
			long retweetId = 0;
			
			while (index < tweetIds.size()) {
			try {
				
				if (tweetIds.get(index).isEmpty() || tweetIds.get(index).equalsIgnoreCase("None")) {
					 System.err.println("skipping "+retweetId);
					continue;
				}
				
			 retweetId = Long.parseLong(tweetIds.get(index));

			Status likeStatus = twitter.createFavorite(retweetId);
			System.err.println("index "+index+" id "+retweetId+" liked "+likeStatus.isFavorited());
			likedIds.add(tweetIds.get(index).toString());

	
			} catch (TwitterException ex) {
				
				System.err.println(ex.getMessage());
				
				if (ex.getErrorCode()==88) {
					System.err.println("Auyth keys limit exceeded : ");
				}
				else {
					
					System.err.println("issue for the user "+uid_gid+" status id "+retweetId);
					likedIds.add(tweetIds.get(index).toString());
					
				}
				
				
				if (ex.getErrorCode() ==32) {
					System.err.println("issue in the keys :"+consumerKey);
				}

				index++;
//				continue;
			}
			index++;
		}
			
				ttafResponse = new TtafResponse(likedIds);
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + uid_gid+ "_likedIds")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				System.err.println(" like Done for "+ uid_gid+" ..! "+likedIds);
				ttafResponse = null;
		}
		
		System.err.println(" like Done for All ..!");
		
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