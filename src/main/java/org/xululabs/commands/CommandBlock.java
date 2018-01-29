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
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.User;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "block", commandDescription = "block user on twitter")
public class CommandBlock extends BaseCommand {

	private static Logger log = LogManager.getRootLogger();
	private MysqlApi mysql = new MysqlApi();
	private UtilFunctions UtilFunctions = new UtilFunctions();

	Twitter4jApi twittertApi = new Twitter4jApi();
	@Parameter(names = "-id", description = "get directory of files", required = true)
	private String inputFilepath;
	@Parameter(names = "-od", description = "get directory of files", required = true)
	private String ouputFilepath;
	
	@Parameter(names = "-dbName", description = "get dbName", required = true)
	private String dbName;
	@Parameter(names = "-dbUser", description = "get dbUser", required = true)
	private String dbUser;
	@Parameter(names = "-dbPassword", description = "get dbPass", required = true)
	private String dbPassword;

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
		
		// Function to read files in directory getting all files in directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
		Set<String> uIds = new HashSet<String>();
		System.err.println("cleaned directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("_");
			uIds.add(fileParts[0]+"_"+fileParts[1]);
		}
		
		long min = 60000; //min 1 minute
		long max = 540000; // max 9 minutes
		
		long sleepTime = UtilFunctions.getRandomeValue(min, max);
		
		long minutes = TimeUnit.MILLISECONDS.toMinutes(sleepTime);
		
		System.err.println("sleeping for "+minutes +" minutes , milliseconds :"+sleepTime );
		
		Thread.sleep(sleepTime);
		
		System.err.println("wokeUp from Sleep ...!");
		for (String uid : uIds) {
			
			// checking file exist if not then continue
			File file1 = new File(this.getFilePath()+"/"+uid+"_blockScreenNames");
			
			if (!(file1.exists())) {
				System.err.println("skipping "+uid+" either missing data file");
				continue;
			}
			
			//loading ScreenNames from directory
			List<String> blockScreenNames = UtilFunctions.loadFile(this.getFilePath()+"/"+uid+"_blockScreenNames");
			
			List<String> blockedScreenNames = new ArrayList<String>();
			
			Map<String, Object> consumerKey = mysql.getAuthKeysByUid(this.getDbName(),this.getDbUser(),this.getDbPassword(), uid);
			
			twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
		
			try {
				
				for (String user : blockScreenNames) {
					
					User blockUser = twitter.createBlock(user);

					if (!blockUser.getScreenName().isEmpty()) {
						blockedScreenNames.add(blockUser.getScreenName());
						}
				
				}
				
			} catch (Exception ex) {
				log.error(ex.getMessage());
			}
			
				ttafResponse = new TtafResponse(blockedScreenNames);
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + uid+ "_blockedScreenNames")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				System.err.println("block users Done for "+ uid+" ..!");
				ttafResponse = null;
		}
		
			System.err.println(" block users Done for All ..!");
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