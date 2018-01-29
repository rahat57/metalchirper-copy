package org.xululabs.commands;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "findUnfollowedFollowers", commandDescription = "dump tweets from twitter")
public class CommandUnfollowedFollowers extends BaseCommand {

	int bulkSize = 500;
	private static Logger log = LogManager.getRootLogger();
	private MysqlApi mysql = new MysqlApi();
	private UtilFunctions UtilFunctions = new UtilFunctions();
	private ElasticsearchApi elasticsearch = new ElasticsearchApi();
	Twitter4jApi twittertApi = new Twitter4jApi();
	private String esHost = "localhost";
	private int esPort = 9300;
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

		//getting fileNmaes from directory
		List<String> fileNames =  UtilFunctions.getFileNames(this.getFilePath());
		Set<String> screenNames = new HashSet<String>();

		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("=");
			screenNames.add(fileParts[0]);
		
		}
		

		for (Object screenName : screenNames) {
			
			String pathFriendsIds = this.getFilePath()+"/"+screenName+"=relationIds";
			String pathFollowerIds = this.getFilePath1()+"/"+screenName+"=ESrelationIds";
			
			File file1 = new File(pathFriendsIds);
			File file2 = new File(pathFollowerIds);
			
			if (!(file1.exists()) || !(file2.exists())) {
				System.err.println("skipping "+screenName+" either missing data file");
				continue;
			}
			
			
			
			List<String> loadRelationIds = UtilFunctions.loadFile(pathFriendsIds);
			List<String> loadESRelationIds = UtilFunctions.loadFile(pathFollowerIds);
			
			if (loadESRelationIds.size()==0) {
				continue;
			}
			
			Map<String, ArrayList<String>> relationIds = mapper.readValue(loadRelationIds.get(0),typeRefForRelationIds);
			LinkedList<ArrayList<String>> listParts = new LinkedList<ArrayList<String>>();
			LinkedList<ArrayList<String>> listESParts = new LinkedList<ArrayList<String>>();
			listParts.add(relationIds.get("commonRelation"));
			listParts.add(relationIds.get("nonCommonFollowers"));
			ArrayList<String> followerIds = UtilFunctions.joinList(listParts);
			
			Map<String, ArrayList<String>> eSRelationIds = mapper.readValue(loadESRelationIds.get(0),typeRefForRelationIds);
			boolean enter = false;
			if (eSRelationIds.size()!=0) {
				
				for (String fields : eSRelationIds.keySet()) {
					
				if (fields.equalsIgnoreCase("esCommonRelation")) {

				listESParts.add(eSRelationIds.get("esCommonRelation"));
				listESParts.add(eSRelationIds.get("esNonCommonFollowers"));
				enter = true;
				}
			}
		} 
			else {
				continue;
			}

			ArrayList<String> unfollowedFollowers = new ArrayList<String>();
			
			if (enter) {
				ArrayList<String> esFollowersIds = UtilFunctions.joinList(listESParts);
	
				int check =0;
				for (int j = 0; j < esFollowersIds.size(); j++) {
					
					check = followerIds.contains(esFollowersIds.get(j)) ? 1 : 0;
					if (check == 0) {
						unfollowedFollowers.add(esFollowersIds.get(j).toString());		
					}
				}
			}
			
			
			Map<String,List<String>> unfollowedIds = new HashMap<String, List<String>>();
			
			unfollowedIds.put("unfollowedIds",unfollowedFollowers);
			
			ttafResponse = new TtafResponse(unfollowedIds);
			BufferedWriter bufferedWriter = null;
			bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + screenName+ "=unfollowedIds")));
			write(ttafResponse, bufferedWriter);
			bufferedWriter.close();
			System.err.println("finding unfollowed followers Done for "+screenName+" ..!");
			
		}			

			System.err.println("finding unfollowed followers Done for All ..!");
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