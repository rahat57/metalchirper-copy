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

@Parameters(commandNames = "relationAnalysis", commandDescription = "making relation from firends followers ids")
public class CommandRelationAnalysis extends BaseCommand {

	int bulkSize = 500;
	private static Logger log = LogManager.getRootLogger();
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
		TypeReference<ArrayList<String>> typeRef = new TypeReference<ArrayList<String>>() {	};
		TtafResponse ttafResponse = null;
		Set<String> screenNames = new HashSet<String>();
		List<String> friendIds = new ArrayList<String>();
		List<String> followerIds = new ArrayList<String>();
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		
		//getting fileNames in a given directory
		List<String> fileNames =  UtilFunctions.getFileNames(this.getFilePath());
		
		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("=");
			screenNames.add(fileParts[0]);
		
		}
		
		for (Object screenName : screenNames) {

			String pathFriendsIds = this.getFilePath()+"/"+screenName+"=friendIds";
			String pathFollowerIds = this.getFilePath1()+"/"+screenName+"=followerIds";
			
			File file1 = new File(pathFriendsIds);
			File file2 = new File(pathFollowerIds);
			if (!(file1.exists()) || !(file2.exists())) {
				System.err.println("skipping "+screenName+" either missing one file");
				continue;
			}
			
			friendIds = UtilFunctions.loadFile(pathFriendsIds);
			followerIds = UtilFunctions.loadFile(pathFollowerIds);
			
			if (friendIds.size()==0 || followerIds.size()==0) {
				
				System.err.println("user "+screenName+" skipped, size 0 for frined or follower ids");
				continue;
				
			}
			friendIds = mapper.readValue(friendIds.get(0), typeRef);
			followerIds = mapper.readValue(followerIds.get(0), typeRef);
			
				int var;
				ArrayList<String> commonRelation = new ArrayList<String>();
				ArrayList<String> friendsNonCommon = new ArrayList<String>();
				Map<String,ArrayList<String>> relationIds = new HashMap<String,ArrayList<String>>();
				
				//finding common and nonCommon friends 
				for (int i = 0; i < friendIds.size(); i++) {
					var = followerIds.contains(friendIds.get(i)) ? 1 : 0;
					if (var == 1) {
						commonRelation.add(friendIds.get(i));
					} else {
						friendsNonCommon.add(friendIds.get(i));
					}
							
				}
				
				ArrayList<String> followersNonCommon = new ArrayList<String>();
				
				//finding  nonCommon followers 
				for (int i = 0; i < followerIds.size(); i++) {
					var = commonRelation.contains(followerIds.get(i)) ? 1 : 0;
					if (var == 0) {
						followersNonCommon.add(followerIds.get(i));
		
					}
				}
				
				relationIds.put("commonRelation", commonRelation);
				relationIds.put("nonCommonFriends", friendsNonCommon);
				relationIds.put("nonCommonFollowers", followersNonCommon);
				System.err.println("commonRelation "+commonRelation.size());
				System.err.println("nonCommonFriends "+friendsNonCommon.size());
				System.err.println("nonCommonFollowers "+followersNonCommon.size());
				
				ttafResponse = new TtafResponse(relationIds);
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + screenName+ "=relationIds")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				System.err.println("relation Analysis Done for "+screenName+" ..!");
		}
		
		System.err.println("relation Analysis Done for All ..!");
		ttafResponse = null;
		
		return ttafResponse;
	}

	@Override
	public void write(TtafResponse ttafResponse, BufferedWriter writer)
			throws Exception {
		Map<String,List<String>> relationIds = (HashMap<String,List<String>>) ttafResponse.getResponseData();
	      
        String jsonSettings = new Gson().toJson(relationIds);
        writer.append(jsonSettings);
        writer.newLine();
		
			
	}
	public boolean	updateMappings(String indexEncription,ArrayList<String> commonRelation,ArrayList<String> followersNonCommon,ArrayList<String> friendsNonCommon,ArrayList<String> esCommonRelation,ArrayList<String> esNonCommonFollower,ArrayList<String> esNonCommonFriend){
		boolean success = false;
		
		//finding followers who Unfollowed in mutual relation
		ArrayList<String> mutualUnFollowed = new ArrayList<String>();
		try {
			int check ;
			for (int i = 0; i < esCommonRelation.size(); i++) {
				
				check = commonRelation.contains(esCommonRelation.get(i)) ? 1 : 0;
				if (check == 0) {
					mutualUnFollowed.add(esCommonRelation.get(i).toString());						}
			}
		
			
		// getting those who Unfolloed me in mutualRelation type
		String [] mutualUnFollowedIds = getArrayIds(mutualUnFollowed);
	if (mutualUnFollowedIds.length >= 1) {
		ArrayList<Map<String, Object>> copycommonFollowers = this.elasticsearch.searchUserDocumentsByIds(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription , "commonrelation", mutualUnFollowedIds);
		
		  // keeping mutualRelation unFollowed record in unfollowedFollowers type and deleting from mutualRelation
			indexInESearch(copycommonFollowers, indexEncription, "unfollowedfollowers");
			
			// deleting from mutualRelation
			deleteFromES(indexEncription, "commonrelation", mutualUnFollowed);
	}
		

		//finding followers who Unfollowed in nonCommonFollowers
		ArrayList<String> nonCommonUnFollowedFollowers = new ArrayList<String>();
		
			for (int i = 0; i < esNonCommonFollower.size(); i++) {
				
				check = followersNonCommon.contains(esNonCommonFollower.get(i)) ? 1 : 0;
				if (check == 0) {
					nonCommonUnFollowedFollowers.add(esNonCommonFollower.get(i).toString());						}
			}
		
			// getting those who Unfolloed me in noncommonfollowers type
			String [] nonCommonUnFollowedFollowersIds = getArrayIds(nonCommonUnFollowedFollowers);
			if (nonCommonUnFollowedFollowersIds.length >= 1) {
				
				ArrayList<Map<String, Object>> copyUnfollowedNonCommonFollowers = this.elasticsearch.searchUserDocumentsByIds(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription , "noncommonfollowers", nonCommonUnFollowedFollowersIds);
				
				// keeping mutualRelation unFollowed record in unfollowedFollowers type and deleting from mutualRelation
				indexInESearch(copyUnfollowedNonCommonFollowers, indexEncription, "unfollowedfollowers");
					
					// deleting from mutualRelation

					deleteFromES(indexEncription, "noncommonfollowers", nonCommonUnFollowedFollowers);

			}
				
		//finding friends who Unfollowed in nonCommonFriends
		
		ArrayList<String> nonCommonUnFollowedFriends = new ArrayList<String>();
		
			for (int i = 0; i < esNonCommonFriend.size(); i++) {
				
				check = friendsNonCommon.contains(esNonCommonFriend.get(i)) ? 1 : 0;
				if (check == 0) {
					nonCommonUnFollowedFriends.add(esNonCommonFriend.get(i).toString());						}
			}
			
		// getting those who Unfolloed me in noncommonfriends type
		String [] nonCommonUnFollowedFriendsIds = getArrayIds(nonCommonUnFollowedFriends);
	
				if (nonCommonUnFollowedFriendsIds.length >= 1) {
					// deleting from mutualRelation
					deleteFromES(indexEncription, "noncommonfriends", nonCommonUnFollowedFriends);
				}		
		
		
		} catch (Exception e) {
			
			System.err.println(e.getMessage());

		}
			
		return success;
	}
	
	 public String[] getArrayIds(ArrayList<String> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
	 
	
	 /**
		 * use to index tweets in ES
		 * 
		 * @param tweets
	 * @throws Exception 
		 */
			public void indexInESearch(ArrayList<Map<String, Object>> tweets,String indexName,String type)throws Exception {
			
				TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
				BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
				for (Map<String, Object> tweet : tweets) {
//					System.err.println(count++ + tweet.toString());
						bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));	
					}				
				bulkRequestBuilder.setRefresh(true).execute().actionGet();
				
				client.close();	
		
		}
			
	/**
	 * use to delete records in ES
	 * 
	 * @param friends followers ids
 * @throws Exception 
	 */
	public void deleteFromES(String indexName,String type,ArrayList<String> ids) throws Exception {
		TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (String id : ids) {
			bulkRequestBuilder.add(client.prepareDelete(indexName,type,id));
		}
		bulkRequestBuilder.setRefresh(true).execute().actionGet();

		client.close();
	}

}