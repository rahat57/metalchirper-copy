package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
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
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "filterFriendsToUnfollow", commandDescription = "dump tweets from twitter")
public class CommandFilterFriendsToUnfollow extends BaseCommand {

	private static Logger log = LogManager.getRootLogger();
	private UtilFunctions UtilFunctions = new UtilFunctions();
	Twitter4jApi twittertApi = new Twitter4jApi();
	MysqlApi mysql = new MysqlApi();
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
	@Parameter(names = "-id", description = "get directory of files", required = true)
	private String inputFilepath;
	@Parameter(names = "-id1", description = "get directory of files", required = true)
	private String inputFilepath1;
	@Parameter(names = "-od", description = "get directory of files", required = true)
	private String ouputFilepath;
	

	public String getFilePath() {
		return inputFilepath;
	}

	public void setFilePath1(String filepath1) {
		this.inputFilepath1 = filepath1;
	}
	
	public String getFilePath1() {
		return inputFilepath1;
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
		
		ObjectMapper mapper = new ObjectMapper();
		TypeReference<HashMap<String,List<String>>> typeRefForRelationIds = new TypeReference<HashMap<String,List<String>>>() {	};

		TtafResponse ttafResponse = null;
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		Set<String> userScreenNames = new HashSet<String>();
		
		//getting fileNmaes from directory
		List<String> screenNames =  UtilFunctions.getFileNames(this.getFilePath());
		
		//parsing filenames and keeping screenNames unique
		for (int f = 0; f <screenNames.size(); f++) {
			String fileParts[] = screenNames.get(f).split("=");
			userScreenNames.add(fileParts[0]);
		
		}

		//iteration over each screenName and getting required file for filtering
		for (Object screenName : userScreenNames) {
		
			String userId_gid = mysql.getGidByScreenName(dbName, dbUser, dbPassword, screenName.toString());
			System.err.println("processing user - "+screenName);
			// making path with complete filename required to get Ids
			String pathCommonIds = this.getFilePath()+"/"+screenName+"=relationIds";
			String pathOldFriendIds = this.getFilePath1()+"/"+userId_gid+"=oldFriendIds";
			
			
			//checking if files exist
			File file1 = new File(pathCommonIds);
			File file2 = new File(pathOldFriendIds);
			if (!(file1.exists()) || !(file2.exists())) {
				System.err.println("skipping "+screenName+" either missing one file");
				continue;
			}
			
			List<String> loadCommonRelationIds = UtilFunctions.loadFile(pathCommonIds);
			List<String> loadOldFriendIds = UtilFunctions.loadFile(pathOldFriendIds);
			 
			if ( loadOldFriendIds.size() == 0 || loadCommonRelationIds.size() == 0 ) {
				continue;
			}
			
			Map<String, ArrayList<String>> relationIds = mapper.readValue(loadCommonRelationIds.get(0),typeRefForRelationIds);

			ArrayList<String> commonRelationids = relationIds.get("commonRelation");

			ArrayList<String> friendApprovedToUnfollow = new ArrayList<String>();
	
				int check =0;
				for (int j = 0; j < loadOldFriendIds.size(); j++) {
					
					check = commonRelationids.contains(loadOldFriendIds.get(j)) ? 1 : 0;
					
					if (check == 0) {
						friendApprovedToUnfollow.add(loadOldFriendIds.get(j).toString());		
					}
				}

			
			Map<String,List<String>> unfollowedIds = new HashMap<String, List<String>>();
			
			unfollowedIds.put("friendsApprovedToUnfollow",friendApprovedToUnfollow);
			
			
			ttafResponse = new TtafResponse(unfollowedIds);
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + userId_gid+ "=friendsApprovedToUnfollow")));
			write(ttafResponse, bufferedWriter);
			bufferedWriter.close();
			System.err.println("filtering Done For "+screenName+" ..!");
			
		}			

			System.err.println("filtering Done for All ..!");
			ttafResponse = null;
			
			return ttafResponse;
	}

	@Override
	public void write(TtafResponse ttafResponse, BufferedWriter writer)
			throws Exception {
		Map<String,List<String>> unfollowedFollowerIds = (HashMap<String,List<String>>) ttafResponse.getResponseData();
	      
        String jsonSettings = new Gson().toJson(unfollowedFollowerIds);
        writer.append(jsonSettings);
        writer.newLine();
		
			
	}
}