package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.UtilFunctions;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.User;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.mysql.cj.fabric.xmlrpc.base.Array;

@Parameters(commandNames = "deleteTweets", commandDescription = "use to delete tweets from elasticsearch older than 3 days")
public class CommandDeleteTweets extends BaseCommand {

	private static Logger log = LogManager.getRootLogger();

	private UtilFunctions UtilFunctions = new UtilFunctions();
	private ElasticsearchApi elasticsearch = new ElasticsearchApi();

	private String esHost = "localhost";
	private int esPort = 9300;
	
	@Parameter(names = "-id", description = "get directory of files", required = true)
	private String inputFilepath;
	@Parameter(names = "-od", description = "get directory of files", required = true)
	private String ouputFilepath;
	@Parameter(names = "-date", description = "get date to delete records", required = true)
	private String date;
	
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
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	
	@Override
	public TtafResponse execute() throws Exception {

		
		TtafResponse ttafResponse = null;
		List<String> successEncriptedNames = new ArrayList<String>();
		try {
			
			// Function to read files in directory getting all files in directory
				List<String> fileNames = UtilFunctions.getFileNames(this.getFilePath());
				
				File file1 = new File(this.getFilePath()+"/"+fileNames.get(0));
				List<String> userIds_gids = new ArrayList<>();
				if (file1.exists()) {
					
					// loading file which contain index Encryption for all users
					userIds_gids = UtilFunctions.loadFile(this.getFilePath()+"/indexEncription");
			}
				else {
						System.err.println("unable to find data file");
				}
					
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.DATE, -3);
				 String keyword=  format.format(cal.getTime()).toString();
				 String type ="tweets";
				
				    
				    
				  // iterating over each user index and deleting all types containing tweets older than 3 days
				  for (String userId_gid : userIds_gids) {
				     boolean success = false;
				     String indexEncription = "tweets_"+userId_gid;
				     if (date.isEmpty()) {
							date = keyword;
						}
				     success = this.elasticsearch.deleteOldTweetsByDate(elasticsearch.getESInstance(esHost, esPort),indexEncription,type,date);
					     
					  if (success) {
					   	 successEncriptedNames.add(indexEncription);
					     System.err.println(" deletion old records done for "+indexEncription);
					}
					    else {
					    	  
						System.err.println("not founding data or something went wrong for "+indexEncription);
					}
				}
				  
				
			} catch (Exception ex) {
				log.error(ex.getMessage());
			}
			
				ttafResponse = new TtafResponse(successEncriptedNames);
				BufferedWriter bufferedWriter = null;
				System.err.println("cleaned directory before writting "+UtilFunctions.cleanDirectory(this.getOuputFilePath()));
				bufferedWriter = new BufferedWriter(new FileWriter(new File(this.getOuputFilePath() + "/encriptedSuccess")));
				write(ttafResponse, bufferedWriter);
				bufferedWriter.close();
				
				ttafResponse = null;
		
		
		System.err.println(" deleting Done for All ..!");
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