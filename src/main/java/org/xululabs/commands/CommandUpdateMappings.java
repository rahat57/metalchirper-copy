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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;
import org.xululabs.datasources.UtilFunctions;

import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "updateMappings", commandDescription = "follow user on twitter")
public class CommandUpdateMappings extends BaseCommand {

	private static Logger log = LogManager.getRootLogger();
	private MysqlApi mysql = new MysqlApi();
	private UtilFunctions UtilFunctions = new UtilFunctions();
	private ElasticsearchApi elasticsearch = new ElasticsearchApi();
	Twitter4jApi twittertApi = new Twitter4jApi();
	
	private String esHost = "localhost";
	private int esPort = 9300;
	
	@Parameter(names = "-dbName", description = "get dbName", required = true)
	private String dbName;
	@Parameter(names = "-dbUser", description = "get dbUser", required = true)
	private String dbUser;
	@Parameter(names = "-dbPassword", description = "get dbPass", required = true)
	private String dbPassword;
	@Parameter(names = "-id", description = "get directory of files", required = true)
	private String inputFilepath;
	@Parameter(names = "-id1", description = "get directory of files", required = true)
	private String inputFilepath1;
	@Parameter(names = "-id2", description = "get directory of files", required = true)
	private String inputFilepath2;
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
	
	public String getFilePath1() {
		return inputFilepath1;
	}

	public void setFilePath1(String filepath1) {
		this.inputFilepath1 = filepath1;
	}
	
	public String getFilePath2() {
		return inputFilepath2;
	}

	public void setFilePath2(String filepath2) {
		this.inputFilepath2 = filepath2;
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
		ObjectMapper mapper = new ObjectMapper();
		TypeReference<HashMap<String,List<String>>> typeRefForRelationIds = new TypeReference<HashMap<String,List<String>>>() {	};
		Map<String, Object> consumerKey = null;
		
		Twitter twitter = null;
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));

		Set<String> screenNames = new HashSet<String>();
		List<String> userIndexed = new ArrayList<String>();
		
		//getting fileNames in a given directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());

		//parsing filenames and keeping screenNames unique
		for (int f = 0; f <fileNames.size(); f++) {
			String fileParts[] = fileNames.get(f).split("=");
			screenNames.add(fileParts[0].trim());
		}
		
		consumerKey = mysql.getAuthKeys(this.getDbName(),this.getDbUser(),this.getDbPassword());
		
    	mysql.updateTimeStamp(this.getDbName(),this.getDbUser(),this.getDbPassword(), consumerKey.get("consumerKey").toString());
    	
    	twitter = twittertApi.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	

		
		//iteration over each screenNames and update mappings from elasticsearch
		for (String screenName : screenNames) {
			
			System.err.println(screenName);
			
			try {
			
				List<String> allScreenNames = mysql.getAllScreenNames(this.getDbName(),this.getDbUser(),this.getDbPassword());
			
				
				if (!(allScreenNames.contains(screenName))) {
					
					System.err.println(screenName+" missing from user_info");
					continue;
				}
				
			// getting Authentication keys for specific user
			Map<String, Object> indexEncriptionAndUis =  mysql.getUidAndIndexEncryption(this.getDbName(),this.getDbUser(),this.getDbPassword(), screenName);
			String indexEncription = indexEncriptionAndUis.get("encrypted").toString();
			
			System.err.println(indexEncription);
			
			ArrayList<String> followersNonCommon = new ArrayList<String>();
			ArrayList<String> commonRelation = new ArrayList<String>();
			ArrayList<String> friendsNonCommon = new ArrayList<String>();
			ArrayList<String> unfollowedFollowers = new ArrayList<String>();
			ArrayList<String> esCommonRelation = new ArrayList<String>();
			ArrayList<String> esNonCommonFollower = new ArrayList<String>();
			ArrayList<String> esNonCommonFriend = new ArrayList<String>();
			
			// making path with complete filename required to update mappings
			String pathCurrentFriendsIds = this.getFilePath()+"/"+screenName+"=relationIds";
			String pathpreviousFollowerIds = this.getFilePath1()+"/"+screenName+"=ESrelationIds";
			String pathUnfollowedFollowerIds = this.getFilePath2()+"/"+screenName+"=unfollowedIds";
			
			
			//checking if files exist in all directory
			File file1 = new File(pathCurrentFriendsIds);
			File file2 = new File(pathpreviousFollowerIds);
			File file3 = new File(pathUnfollowedFollowerIds);
			
			if (!(file1.exists())) {
				System.err.println("skipping "+screenName+" either missing data file");
				continue;
			}
			
			List<String> loadESRelationIds = new ArrayList<String>();
			if (file2.exists()) {
				loadESRelationIds = UtilFunctions.loadFile(pathpreviousFollowerIds);
				System.err.println("loaded ES old DATA");

			}
			
			List<String> loadUnfollowedFollowerids = new ArrayList<String>();
			if (file3.exists()) {
				loadUnfollowedFollowerids = UtilFunctions.loadFile(pathUnfollowedFollowerIds);
				System.err.println("loaded unfollowed followers DATA");

			}
			
			//loading json as a string and then make it proper json object
			List<String> loadRelationIds = UtilFunctions.loadFile(pathCurrentFriendsIds);
			
			Map<String, ArrayList<String>> relationIds = mapper.readValue(loadRelationIds.get(0),typeRefForRelationIds);
//			System.err.println("current relation ids "+relationIds);
			
			commonRelation = relationIds.get("commonRelation");
			followersNonCommon = relationIds.get("nonCommonFollowers");
			friendsNonCommon =  relationIds.get("nonCommonFriends");
			
			Map<String, ArrayList<String>> eSRelationIds = null;
			if (!(loadESRelationIds.size() == 0) ) {
				
				eSRelationIds = mapper.readValue(loadESRelationIds.get(0),typeRefForRelationIds);
				System.err.println("ESrelationSize "+eSRelationIds.size());
				esCommonRelation = eSRelationIds.get("esCommonRelation");
				esNonCommonFollower = eSRelationIds.get("esNonCommonFollowers");
				esNonCommonFriend =  eSRelationIds.get("esNonCommonFriends");
//				System.err.println("esRelationIds "+eSRelationIds);
			}
			
			Map<String, ArrayList<String>> unfollowedfollowers = new HashMap<String, ArrayList<String>>();
			
			if (!(loadUnfollowedFollowerids.size() == 0)) {
				unfollowedfollowers = mapper.readValue(loadUnfollowedFollowerids.get(0),typeRefForRelationIds);
				unfollowedFollowers = unfollowedfollowers.get("unfollowedIds");
				System.err.println("unfollowed followers size "+unfollowedFollowers.size() );

			}
			
		
		
			//if new user he doesn't have previous data then no need to update mappings
			if ( esCommonRelation.size() == 0 && esNonCommonFollower.size() == 0 && esNonCommonFriend.size() == 0) {
				
				System.err.println(screenName+" new user skipping updateMapping");
				
			}
			 else {
				 
				 System.out.println("updating");
				updateMappings(indexEncription, commonRelation, followersNonCommon, friendsNonCommon, esCommonRelation, esNonCommonFollower, esNonCommonFriend);
				
			}
			
			//getting user info from twitter and index into elasticsearch for further use
			ArrayList<Map<String, Object>> userInfo = new ArrayList<Map<String,Object>>();
			
				userInfo = twittertApi.getUserInfo(twitter, screenName);
			
				Map<String, Object> updateRelation = new HashMap<String, Object>();
				String userId = userInfo.get(0).get("id").toString();
				updateRelation.putAll(userInfo.get(0));
				updateRelation.put("commonRelation",commonRelation);
				updateRelation.put("nonCommonFriends",friendsNonCommon);
				updateRelation.put("nonCommonFollowers",followersNonCommon);
				updateRelation.put("unfollowedFollowers",unfollowedFollowers);
				
//				System.err.println("relation updating fro "+screenName+" object "+updateRelation);
				
				TransportClient client1 = this.elasticsearch.getESInstance(this.esHost, this.esPort);
				client1.prepareUpdate(indexEncription,"user",userId).setDoc(updateRelation).setUpsert(updateRelation).setRefresh(true).execute().actionGet();
				client1.close();
				
				//
				 long commonRelationlong [] = UtilFunctions.getArrayListAslong(commonRelation);
				 long nonCommonFriends [] = UtilFunctions.getArrayListAslong(friendsNonCommon);
				 long nonCommonFollowers [] = UtilFunctions.getArrayListAslong(followersNonCommon);
				 
				 LinkedList<long[]> commonRelationChunks = UtilFunctions.chunks(commonRelationlong, 500);
				 LinkedList<long[]> nonCommonFriendsChunks = UtilFunctions.chunks(nonCommonFriends, 500);
				 LinkedList<long[]> nonCommonFollowersChunks = UtilFunctions.chunks(nonCommonFollowers, 500);
				Boolean userRelationIndexCreated =false;
				
				
				String relationIndexName = "relation_"+userId;
				// creating userRelationIndex
				userRelationIndexCreated = elasticsearch.createUserRelationIndex(relationIndexName, "user_relation", commonRelationChunks,nonCommonFollowersChunks,nonCommonFriendsChunks, commonRelation, followersNonCommon,friendsNonCommon);

				if (userRelationIndexCreated) {
					System.err.println("Relation Index Created For User: "+screenName);
				}
				
				userIndexed.add(screenName);
				
			}
			catch (TwitterException ex) {
				System.err.println(ex.getMessage());
				log.error(ex.getMessage());
			}
			catch (Exception ex) {
				System.err.println(ex.getMessage());
				log.error(ex.getMessage());
			}
		
			
			System.out.println("update mappings done for user "+screenName);
		}
		
		System.err.println("updated "+userIndexed);
		ttafResponse = new TtafResponse(userIndexed);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" +"mappingUpdated")));
		write(ttafResponse, bufferedWriter);
		bufferedWriter.close();
		System.out.println("update mappings Done for All ..!");
		ttafResponse = null;
		return ttafResponse;
	}

	@Override
	public void write(TtafResponse ttafResponse, BufferedWriter writer)
			throws Exception {
		List<String> userIndexed = (ArrayList<String>) ttafResponse.getResponseData();
        String jsonSettings = new Gson().toJson(userIndexed);
        writer.append(jsonSettings);
        writer.newLine();
		
			
	}
	
	// create new index for only user relation if already exist delete it and recreate

	 public boolean	createUserRelationIndex(String indexEncription,String type,LinkedList<long[]> commonRelationChunks,LinkedList<long[]> nonCommonFollowersChunks,LinkedList<long[]> nonCommonFriendsChunks,ArrayList<String> commonRelation,ArrayList<String> followersNonCommon,ArrayList<String> friendsNonCommon){
			
		 boolean relationDocumentscreated = false;
		
		
		 Map<String, Object> userRelation = new HashMap<String,Object>();
		
		//

		try {
			
			// deleting existing index and recreating and adding data
			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			
			 boolean IndexExist = elasticsearch.indexExist(client, indexEncription);
			 
			 if (IndexExist) {
				 
				 elasticsearch.deleteIndex(client, indexEncription);	
			}
			 
			 // for common followers relation value 1
			 userRelation.put("relation", 1);

			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

			 for (int i = 0; i < commonRelationChunks.size(); i++) {
				 
				 long commanArray[] = commonRelationChunks.get(i);
				 
				for (int j = 0; j < commanArray.length-1; j++) {
					
					String id = Long.toString(commanArray[j]);
					
					bulkRequestBuilder.add(client.prepareUpdate(indexEncription,type,id).setDoc(userRelation).setUpsert(userRelation));
				}
				bulkRequestBuilder.setRefresh(true).execute().actionGet();
			}
			 
			 
			// for nonCommon followers relation value 1
			userRelation.put("relation", 2);

					 for (int i = 0; i < nonCommonFollowersChunks.size(); i++) {
						 
						 long nonCommanArray[] = nonCommonFollowersChunks.get(i);
						 
						for (int j = 0; j < nonCommanArray.length-1; j++) {
							
							String id = Long.toString(nonCommanArray[j]);
							
							bulkRequestBuilder.add(client.prepareUpdate(indexEncription,type,id).setDoc(userRelation).setUpsert(userRelation));
						}
						
						bulkRequestBuilder.setRefresh(true).execute().actionGet();
					}
					 
					// for nonCommon friends relation value 1
						userRelation.put("relation", 3);

				 for (int i = 0; i < nonCommonFriendsChunks.size(); i++) {
									 
					 long nonCommanArray[] = nonCommonFriendsChunks.get(i);
									 
					for (int j = 0; j < nonCommanArray.length-1; j++) {
										
							String id = Long.toString(nonCommanArray[j]);
										
							bulkRequestBuilder.add(client.prepareUpdate(indexEncription,type,id).setDoc(userRelation).setUpsert(userRelation));
						}
									
						bulkRequestBuilder.setRefresh(true).execute().actionGet();
					} 
			 

					

					String[] credentialcommonIds = getArrayIds(commonRelation);
					String[] credentialNonCommonFollowerIds = getArrayIds(followersNonCommon);
					String[] credentialNonCommonFriendsIds = getArrayIds(friendsNonCommon);
					
					
			if (elasticsearch.documentsExist(client,indexEncription, type, credentialcommonIds) && elasticsearch.documentsExist(client,indexEncription,type, credentialNonCommonFollowerIds)&& elasticsearch.documentsExist(client,indexEncription, type, credentialNonCommonFriendsIds) ) {
					
				relationDocumentscreated = true;
				
			}
			 
		
		} catch (Exception e) {
			
			log.error(e.getMessage());

		}
			
		return relationDocumentscreated;
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
		
		if (copycommonFollowers.size() > 0) {
			
			// keeping mutualRelation unFollowed record in unfollowedFollowers type and deleting from mutualRelation
						indexInESearch(copycommonFollowers, indexEncription, "unfollowedfollowers");
						
						// deleting from mutualRelation
						deleteFromES(indexEncription, "commonrelation", mutualUnFollowed);
		}
		  
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
				
				if (copyUnfollowedNonCommonFollowers.size() > 0) {
					// keeping mutualRelation unFollowed record in unfollowedFollowers type and deleting from mutualRelation
					indexInESearch(copyUnfollowedNonCommonFollowers, indexEncription, "unfollowedfollowers");
						
						// deleting from mutualRelation

						deleteFromES(indexEncription, "noncommonfollowers", nonCommonUnFollowedFollowers);

				}
			
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
				
				// Deleting those Unfollowed followers who follow back again in unfollowed followers type
				// deleting common followers from unfollowed type
						if (commonRelation.size() >= 1) {
							// deleting from mutualRelation
							deleteFromES(indexEncription, "unfollowedfollowers", commonRelation);
						}	
						
				// deleting nonCommon followers from unfollowed type
					if (commonRelation.size() >= 1) {
						// deleting from mutualRelation
						deleteFromES(indexEncription, "unfollowedfollowers", followersNonCommon);
					
					}	
		
		
		} catch (Exception e) {
			
			log.error(e.getMessage());

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