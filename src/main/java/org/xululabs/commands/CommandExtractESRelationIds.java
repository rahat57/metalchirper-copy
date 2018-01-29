package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;

@Parameters(commandNames = "getEsrelationIds", commandDescription = "extract last date friends follower ids from elasticSearch")
public class CommandExtractESRelationIds extends BaseCommand {

	int bulkSize = 500;
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
		
		TypeReference<ArrayList<String>> typeRef = new TypeReference<ArrayList<String>>() {	};
		TtafResponse ttafResponse = null;
		
		System.err.println("cleaned ouput directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
		Map<String, List<String>> esRelationIds = new HashMap<String, List<String>>();
		
		// Function to read files in directory getting all files in directory
		List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
		
		//loading all fileNames to get their data from elasticsearch
		List<String> screenNames = UtilFunctions.loadFile(this.getFilePath() + "/"+ fileNames.get(0));

		// iterate over screenNames to get data from elasticsearch
		for (Object screenName : screenNames) {
			
			System.err.println(screenName);
			
			//getting index encryption from database
			Map<String, Object> uidIndexEcryption = mysql.getUidAndIndexEncryption(this.getDbName(),this.getDbUser(),this.getDbPassword(), screenName.toString().trim());
			String userId =uidIndexEcryption.get("uid").toString();
			String indexEncription = uidIndexEcryption.get("encrypted").toString();
			
			
				String[]  userid= {userId};
				
				TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
				ArrayList<Map<String, Object>> unfollowed = this.elasticsearch.searchUserDocumentsByIds(client,indexEncription, "user",userid);
				LinkedList<ArrayList<String>> esTotalFollowers = null;
				
				//finding  followers who unfollowed me

				ArrayList<String> esCommonRelation = new ArrayList<String>();
				ArrayList<String> esNonCommonFollower = new ArrayList<String>();
				ArrayList<String> esNonCommonFriend = new ArrayList<String>();

				if (unfollowed.size()!=0) {
					
					for (String fields : unfollowed.get(0).keySet()) {
						
						if (fields.equalsIgnoreCase("commonRelation") || fields.equalsIgnoreCase("nonCommonFollowers")) {

							esCommonRelation = (ArrayList<String>) unfollowed.get(0).get("commonRelation");
							esNonCommonFollower = (ArrayList<String>) unfollowed.get(0).get("nonCommonFollowers");
							esNonCommonFriend = (ArrayList<String>) unfollowed.get(0).get("nonCommonFriends");
							esTotalFollowers = new LinkedList<ArrayList<String>>();
							esTotalFollowers.add(esCommonRelation);
							esTotalFollowers.add(esNonCommonFollower);
						}
						
					}			
				}
				
				esRelationIds.put("esCommonRelation",esCommonRelation );
				esRelationIds.put("esNonCommonFollowers", esNonCommonFollower);
				esRelationIds.put("esNonCommonFriends", esNonCommonFriend);

				ttafResponse = new TtafResponse(esRelationIds);
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/" + screenName+ "=ESrelationIds")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				esRelationIds.clear();
				System.err.println("ES relationIDs Done for "+ screenName+" ..!");
				ttafResponse = null;
		}
		
		System.err.println("ESrelationIDs Done for All ..!");
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