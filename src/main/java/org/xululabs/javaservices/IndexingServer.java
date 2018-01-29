package org.xululabs.javaservices;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.Twitter4jApi;
import org.xululabs.datasources.UtilFunctions;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import twitter4j.Twitter;
import twitter4j.TwitterException;

public class IndexingServer extends AbstractVerticle {
	private static Logger log = LogManager.getRootLogger();
	HttpServer server;
	Router router;
	Twitter4jApi twitter4jApi;
	String host;
	int port;
	ElasticsearchApi elasticsearch;
	MysqlApi mysql;
	UtilFunctions UtilFunctions;
	String dbName;
	String dbUser;
	String dbPassword;
	String esHost;
	String type;
	int esPort;
	int bulkSize = 700;

	/**
	 * constructor use to initialize values
	 */
	 public  IndexingServer()  {
			this.host = "localhost";
			this.port =8182;
			this.twitter4jApi = new Twitter4jApi();
			this.elasticsearch = new ElasticsearchApi();
			this.mysql = new MysqlApi();
			this.UtilFunctions = new UtilFunctions();
			this.esHost = "localhost";
			this.esPort = 9300;
			this.type = "tweets"; 
			this.bulkSize = 500;
			this.dbName = "mchirper"; //metalchirper  mchirper
			this.dbUser = "mchirper_owner"; //mchirper_owner   root
			this.dbPassword ="z1EhgV6C7kDC"; //z1EhgV6C7kDC    ""
		
	}

	/**
	 * Deploying the verical
	 */
	@Override
	public void start() {	

		server = vertx.createHttpServer();
		router = Router.router(vertx);
		// Enable multipart form data parsing
		router.route().handler(BodyHandler.create());
		router.route().handler(
				CorsHandler.create("*").allowedMethod(HttpMethod.GET)
						.allowedMethod(HttpMethod.POST)
						.allowedMethod(HttpMethod.OPTIONS)
						.allowedHeader("Content-Type, Authorization"));
		
		// registering different route handlers
		this.registerHandlers();
		
		//portConnection.getPort()
		server.requestHandler(router::accept).listen(port);
		
	}
	
	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {
		router.route(HttpMethod.GET, "/").blockingHandler(this::welcomeRoute);
		router.route(HttpMethod.POST, "/indexTweets").blockingHandler(this::indexTweets);
		router.route(HttpMethod.POST, "/indexUserTimeline").blockingHandler(this::indexUserTimeLine);
		router.route(HttpMethod.POST, "/indexUser").blockingHandler(this::indexUsers);
		router.route(HttpMethod.POST, "/deleteInfluencer").blockingHandler(this::deleteInfluence);
		router.route(HttpMethod.POST, "/userInfo").blockingHandler(this::userInfoRoute);
		router.route(HttpMethod.POST, "/stickyInfo").blockingHandler(this::indexUserInfoRoute);
		router.route(HttpMethod.POST, "/indexUserInfluence").blockingHandler(this::indexUserInfluence);
		router.route(HttpMethod.POST, "/muteUser").blockingHandler(this::muteRoute);
		router.route(HttpMethod.POST, "/unfollowUser").blockingHandler(this::unfollowRoute);
	

	}

	/**
	 * welcome route
	 * 
	 * @param routingContext
	 */

	public void welcomeRoute(RoutingContext routingContext) {
		routingContext.response().end("<h1>Welcome To Metalchirper-v2</h1>");
	}

	/**
	 * use to index User documents
	 * 
	 * @param routingContext
	 */
	public void indexUsers(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexUsersBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}

	/**
	 * use to delete documents
	 * 
	 * @param routingContext
	 */
	public void deleteInfluence(final RoutingContext routingContext) {
		  Thread t=new Thread() {
		   public void run() { 
			   deleteInfluenceBlocking(routingContext);
		   }
		  };
		  t.setDaemon(true);
		  t.start();
		 }
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void userInfoRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				userInfoRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void muteRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				muteRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void unfollowRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				unfollowRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void indexUserInfoRoute(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexUserInfoRouteBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	
	/**
	 * use to index tweets into elasticsearch 
	 * 
	 * @param routingContext
	 */
	public void indexTweets(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexTweetsBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to index tweets into elasticsearch 
	 * 
	 * @param routingContext
	 */
	public void indexUserTimeLine(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexUserTimeLineBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	
	
	/**
	 * use to search User documents
	 * 
	 * @param routingContext
	 */
	public void indexUserInfluence(final RoutingContext routingContext) {
		Thread t=new Thread() {
			public void run() {
				indexUserInfluenceBlocking(routingContext);
			}
		};
		t.setDaemon(true);
		t.start();
	}
	
	/**
	 * use to index user info for given credentials
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexUsersBlocking(RoutingContext routingContext) {
		String response = "";
		Map<String, Object> responseMap = new HashMap<String, Object>(); 
		ObjectMapper mapper = new ObjectMapper();
		String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");
		String userId = (routingContext.request().getParam("userId") == null) ? "" : routingContext.request().getParam("userId");
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
			
		try {
			
					if (indexEncription.isEmpty()) {
						responseMap.put("index missing", "please give the value of the indexEncription");
						log.warn("index missing", "please give the value of the indexEncription");
	
					}
					
			Map<String, Object> credentials = mysql.getAuthKeys(dbName,dbUser,dbPassword);
	    	mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentials.get("consumerKey").toString());
	
			ArrayList<Map<String, Object>> usersInfo = new ArrayList<Map<String,Object>>();
			
			long start = System.currentTimeMillis();
			ArrayList<Map<String, Object>> userScreeName = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),screenName);
								
			// indexing credentials info object into elasticsearch
			TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
			client.prepareUpdate(indexEncription,"user",userId).setDoc(userScreeName.get(0)).setUpsert(userScreeName.get(0)).setRefresh(true).execute().actionGet();
			client.close();
			
			//getting credentials friends and followers ids
			ArrayList<String> followerIds = this.getFollowerIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), screenName);
			credentials = mysql.getAuthKeys(dbName,dbUser,dbPassword);
	    	mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentials.get("consumerKey").toString());
			ArrayList<String> friendIds = this.getFriendIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), screenName);
			
			// updating User relation
			Map<String, ArrayList<String>> relationsIds = updateUserRelation(indexEncription,userId, friendIds, followerIds);
			 
			 System.err.println("Relation Updated...!");
			 
			 long commonRelation [] = UtilFunctions.getArrayListAslong(relationsIds.get("commonRelation"));
			 long nonCommonFriends [] = UtilFunctions.getArrayListAslong(relationsIds.get("nonCommonFriends"));
			 long nonCommonFollowers [] = UtilFunctions.getArrayListAslong(relationsIds.get("nonCommonFollowers"));
			 
			 LinkedList<long[]> commonRelationChunks = UtilFunctions.chunks(commonRelation, 500);
			 LinkedList<long[]> nonCommonFriendsChunks = UtilFunctions.chunks(nonCommonFriends, 500);
			 LinkedList<long[]> nonCommonFollowersChunks = UtilFunctions.chunks(nonCommonFollowers, 500);
			 
			 credentials = mysql.getAuthKeys(dbName,dbUser,dbPassword);
		     mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentials.get("consumerKey").toString());
			 
			// getting common relations id's info by sending 500 ids and then indexing into elasticSearch
			 for (int i = 0; i < commonRelationChunks.size(); i++) {

				 usersInfo = this.getUsersInfoByIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),commonRelationChunks.get(i));
				
				this.indexInESearch(usersInfo,indexEncription,"commonrelation");
			
			}
			 
			 credentials = mysql.getAuthKeys(dbName,dbUser,dbPassword);
		     mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentials.get("consumerKey").toString());
			 
			 // getting followers info by sending 500 ids and then indexing into elasticSearch
			 for (int i = 0; i <nonCommonFollowersChunks.size(); i++) {
				 usersInfo = this.getUsersInfoByIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),nonCommonFollowersChunks.get(i));
				
				this.indexInESearch(usersInfo,indexEncription,"noncommonfollowers");
			
			}
			
			credentials = mysql.getAuthKeys(dbName,dbUser,dbPassword);
		    mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentials.get("consumerKey").toString());
			// getting friends info by sending 500 ids and then indexing into elasticSearch
			 
			 for (int i = 0; i <  nonCommonFriendsChunks.size(); i++) {
				 usersInfo = this.getUsersInfoByIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),nonCommonFriendsChunks.get(i));
					
					this.indexInESearch(usersInfo,indexEncription,"noncommonfriends");
				
				}

			boolean success = false;

			String[] credentialcommonIds = UtilFunctions.getArrayIds(relationsIds.get("commonRelation"));
			String[] credentialNonCommonFollowerIds = UtilFunctions.getArrayIds(relationsIds.get("nonCommonFollowers"));
			String[] credentialNonCommonFriendsIds = UtilFunctions.getArrayIds(relationsIds.get("nonCommonFriends"));
			
			;
			
			if (elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, "commonrelation", credentialcommonIds) && elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, "noncommonfollowers", credentialNonCommonFollowerIds)&& elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, "noncommonfriends", credentialNonCommonFriendsIds) ) {
				success = true;
			}
			
		 
		 Boolean userRelationIndexCreated =false;
		
			String relationIndexName = "relation_"+userId;
			// creating userRelationIndex
			userRelationIndexCreated = elasticsearch.createUserRelationIndex(relationIndexName, "user_relation", commonRelationChunks,nonCommonFollowersChunks,nonCommonFriendsChunks, relationsIds.get("commonRelation"), relationsIds.get("nonCommonFollowers"), relationsIds.get("nonCommonFriends"));
			
	if (success && userRelationIndexCreated) {
			long end = System.currentTimeMillis()-start;
			log.info("time taken for user Indexing total "+end);
			responseMap.put("status", "success");
			response = mapper.writeValueAsString(responseMap);
		}
			
				
		} catch (Exception ex) {
			
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);

	}
	
	
	
public Map<String,ArrayList<String>> updateUserRelation(String indexEncription,String userId,ArrayList<String> friendIds,ArrayList<String> followerIds) throws TwitterException{ // Update User
		
		Map<String,ArrayList<String>> relationIds = new HashMap<String,ArrayList<String>>();
		try {	
		int var;
		ArrayList<String> commonRelation = new ArrayList<String>();
		ArrayList<String> friendsNonCommon = new ArrayList<String>();
		
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
				
		String[]  userid= {userId};
		TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
		ArrayList<Map<String, Object>> unfollowed = this.elasticsearch.searchUserDocumentsByIds(client,indexEncription, "user",userid);
		LinkedList<ArrayList<String>> esTotalFollowers = null;
		
		//finding  followers who unfollowed me
		ArrayList<String> unfollowedFollowers = new ArrayList<String>();
		ArrayList<String> esCommonRelation = new ArrayList<String>();
		ArrayList<String> esNonCommonFollower = new ArrayList<String>();
		ArrayList<String> esNonCommonFriend = new ArrayList<String>();
		boolean enter = false;
		if (unfollowed.size()!=0) {
			
			for (String fields : unfollowed.get(0).keySet()) {
				
				if (fields.equalsIgnoreCase("commonRelation") || fields.equalsIgnoreCase("nonCommonFollowers")) {

					esCommonRelation = (ArrayList<String>) unfollowed.get(0).get("commonRelation");
					esNonCommonFollower = (ArrayList<String>) unfollowed.get(0).get("nonCommonFollowers");
					esNonCommonFriend = (ArrayList<String>) unfollowed.get(0).get("nonCommonFriends");
					esTotalFollowers = new LinkedList<ArrayList<String>>();
					esTotalFollowers.add(esCommonRelation);
					esTotalFollowers.add(esNonCommonFollower);
					enter = true;
					
				}
				
			}
			if (enter) {
				int check ;
				for ( ArrayList<String> esFollowers : esTotalFollowers) {
					
					for (int i = 0; i < esFollowers.size(); i++) {
						
						check = followerIds.contains(esFollowers.get(i)) ? 1 : 0;
						if (check == 0) {
							unfollowedFollowers.add(esFollowers.get(i).toString());						}
					}
				}
				
				//updating all types mutualRelation ,nonCommonFriends,nonCommonFollowers so user get only latest data
				updateMappings(indexEncription,commonRelation, followersNonCommon, friendsNonCommon,esCommonRelation,esNonCommonFollower,esNonCommonFriend);
			}
			
		}
	
		relationIds.put("commonRelation", commonRelation);
		relationIds.put("nonCommonFriends", friendsNonCommon);
		relationIds.put("nonCommonFollowers", followersNonCommon);
		
		Map<String, Object> updateRelation = new HashMap<String, Object>();
		updateRelation.put("commonRelation",commonRelation);
		updateRelation.put("nonCommonFriends",friendsNonCommon);
		updateRelation.put("nonCommonFollowers",followersNonCommon);
		updateRelation.put("unfollowedFollowers",unfollowedFollowers);
		TransportClient client1 = this.elasticsearch.getESInstance(this.esHost, this.esPort);
		client1.prepareUpdate(indexEncription,"user",userId).setDoc(updateRelation).setUpsert(updateRelation).setRefresh(true).execute().actionGet();
		client1.close();
		
	} catch (Exception e) {
		e.printStackTrace();
		log.error(e.getMessage());
//		log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

	}
	return relationIds;
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
		String [] mutualUnFollowedIds = UtilFunctions.getArrayIds(mutualUnFollowed);
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
			String [] nonCommonUnFollowedFollowersIds = UtilFunctions.getArrayIds(nonCommonUnFollowedFollowers);
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
		String [] nonCommonUnFollowedFriendsIds = UtilFunctions.getArrayIds(nonCommonUnFollowedFriends);
	
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
		
/**
	 * use to index tweets in ES
	 * 
	 * @param tweets
 * @throws Exception 
	 */
		public void indexInESearch(ArrayList<Map<String, Object>> tweets,String indexName,String type)throws Exception {
			int count =1;
			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			for (Map<String, Object> tweet : tweets) {
//				System.err.println(count++ + tweet.toString());
					bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));	
				}				
			bulkRequestBuilder.setRefresh(true).execute().actionGet();
			
			client.close();	
	
	}

		
	 public String[] getIds(ArrayList<Object> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
	 
	/**
	 * use to index user influencer info for given credentials
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	
	 public void indexUserInfluenceBlocking(RoutingContext routingContext) {
			String response = "";
			Map<String, Object> responseMap = new HashMap<String, Object>();
			ObjectMapper mapper = new ObjectMapper();
			String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription").toLowerCase();
			String userId1 = (routingContext.request().getParam("userId") == null) ? "" : routingContext.request().getParam("userId");
			String credentialScreename = (routingContext.request().getParam("credentialScreenName") == null) ? "" : routingContext.request().getParam("credentialScreenName").toString().toLowerCase();;
			String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");
			
			try {


				Map<String, Object> credentials = mysql.getAuthKeys(dbName,dbUser,dbPassword);
		    	mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentials.get("consumerKey").toString());
			
				String userId=userId1;
				String influenceScreenName = screenName.toLowerCase().trim();
				long start = System.currentTimeMillis();
				
									
				String[]  userid= {userId};
									
				TransportClient client = elasticsearch.getESInstance(this.esHost, this.esPort);
				ArrayList<Map<String, Object>> documents=null;
				documents = this.userInfo(this.getTwitterInstance(credentials.get("consumerKey").toString(),credentials.get("consumerSecret").toString(),credentials.get("accessToken").toString(),credentials.get("accessTokenSecret").toString()),credentialScreename);
				this.indexInESearch(documents, indexEncription, "user");
				ArrayList<Map<String, Object>> userFollowers = this.elasticsearch.searchUserDocumentsByIds(client,indexEncription, "user",userid);
				LinkedList<ArrayList<String>> esTotalFollowers = null;
									
				//finding  followers who unfollowed me

				ArrayList<String> esCommonRelation = new ArrayList<String>();
				ArrayList<String> esNonCommonFollower = new ArrayList<String>();

				if (userFollowers.size()!=0) {
										
				for (String fields : userFollowers.get(0).keySet()) {
											
				if (fields.equalsIgnoreCase("commonRelation") || fields.equalsIgnoreCase("nonCommonFollowers")) {

				esCommonRelation = (ArrayList<String>) userFollowers.get(0).get("commonRelation");
				esNonCommonFollower = (ArrayList<String>) userFollowers.get(0).get("nonCommonFollowers");
				esTotalFollowers = new LinkedList<ArrayList<String>>();
				esTotalFollowers.add(esCommonRelation);
				esTotalFollowers.add(esNonCommonFollower);
								}
										
						}			
					}
									
				
				
				ArrayList<String> credentialFollowerIds = this.getFollowerIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
				ArrayList<String>influenceFollowerIds = this.getFollowerIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);
				
				
				ArrayList<String>influenceFriendIds = this.getFriendIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);

				
			//finding common and Non common followers ids and saving into credentials user object
				
				Map<String, ArrayList<String>> relationIds = updateUserInfluencerRelation(indexEncription,credentialScreename, influenceScreenName, userId,credentialFollowerIds,influenceFollowerIds,influenceFriendIds);
			
				long InfluenceCommonFollowerids [] = UtilFunctions.getArrayListAslong(relationIds.get("commonRelation"));
				long InfluenceNonCommonFollowerids [] = UtilFunctions.getArrayListAslong(relationIds.get("nonCommonRelation"));
				long InfluencerFriendsWhoFollowMe [] = UtilFunctions.getArrayListAslong(relationIds.get("friendsWhoFollowMe"));
				long InfluencerFriendsNotFollowMe [] = UtilFunctions.getArrayListAslong(relationIds.get("friendsNotFollowMe"));
				
				System.err.println("Relation Updated...!");
				 
				ArrayList<Map<String, Object>> credentialsInfo = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);			
				TransportClient client2 = elasticsearch.getESInstance(this.esHost, this.esPort);
				client2.prepareUpdate(indexEncription,"user",credentialsInfo.get(0).get("id").toString()).setDoc(credentialsInfo.get(0)).setUpsert(credentialsInfo.get(0)).setRefresh(true).execute().actionGet();
				client2.close();
				 
				LinkedList<long[]> commonFollowerschunks = UtilFunctions.chunks(InfluenceCommonFollowerids, 500);
				LinkedList<long[]> nonCommonFollowerschunks = UtilFunctions.chunks(InfluenceNonCommonFollowerids, 500);
				LinkedList<long[]> friendWhoFollowMechunks = UtilFunctions.chunks(InfluencerFriendsWhoFollowMe, 500);
				LinkedList<long[]> friendsWhoNotFollowMechunks = UtilFunctions.chunks(InfluencerFriendsNotFollowMe, 500);
				
				
				 // indexing influencer credentials common followers into elasticsearch in type [credentaialsScreenName+influenceScreennamecommonfollowers] 
				 for (int i = 0; i < commonFollowerschunks.size(); i++) {
					 
					 ArrayList<Map<String, Object>> tweets = this.getUsersInfoByIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),commonFollowerschunks.get(i));
					
					this.indexInESearch(tweets,indexEncription,credentialScreename+influenceScreenName+"commonfollowers");
				
				}
				 
				 // indexing influencer credentials NonCommon followers into elasticsearch in type [credentaialsScreenName+influenceScreennameNonCommonfollowers] 
				 for (int i = 0; i < nonCommonFollowerschunks.size(); i++) {
					 
					 ArrayList<Map<String, Object>> tweets = this.getUsersInfoByIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),nonCommonFollowerschunks.get(i));
					
					this.indexInESearch(tweets,indexEncription,credentialScreename+influenceScreenName+"noncommonfollowers");
				
				}
				 
				// indexing influencer friends who follow credentials into elasticsearch in type [credentaialsScreenName+influenceScreenname+friendswhofollow] 
				 for (int i = 0; i < friendWhoFollowMechunks.size(); i++) {
					 
					 ArrayList<Map<String, Object>> tweets = this.getUsersInfoByIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),friendWhoFollowMechunks.get(i));
					
					this.indexInESearch(tweets,indexEncription,credentialScreename+influenceScreenName+"friendswhofollow");
				
				}
				 
				// indexing influencer friends who not follow credentials into elasticsearch in type [credentaialsScreenName+influenceScreenname+friendsnotfollow] 
				 for (int i = 0; i < friendsWhoNotFollowMechunks.size(); i++) {
					 
					 ArrayList<Map<String, Object>> tweets = this.getUsersInfoByIds(dbName,dbUser,dbPassword,this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),friendsWhoNotFollowMechunks.get(i));
					
					this.indexInESearch(tweets,indexEncription,credentialScreename+influenceScreenName+"friendsnotfollow");
				
				}
				
				boolean success = false;
				String[] commonFollowersIds = UtilFunctions.getArrayIds(relationIds.get("commonRelation"));
				String[] nonCommonFollowerIds = UtilFunctions.getArrayIds(relationIds.get("nonCommonRelation"));
			
				String[] friendsWhoFollowMeIds = UtilFunctions.getArrayIds(relationIds.get("friendsWhoFollowMe"));
				String[] friendsNotFollowMeIds = UtilFunctions.getArrayIds(relationIds.get("friendsNotFollowMe"));
				
				if (elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, credentialScreename+influenceScreenName+"commonfollowers", commonFollowersIds) && elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, credentialScreename+influenceScreenName+"noncommonfollowers", nonCommonFollowerIds) && elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, credentialScreename+influenceScreenName+"friendswhofollow", friendsWhoFollowMeIds) && elasticsearch.documentsExist(this.elasticsearch.getESInstance(this.esHost, this.esPort),indexEncription, credentialScreename+influenceScreenName+"friendsnotfollow", friendsNotFollowMeIds) ) {
					
					success = true;
					
				}		
				
					
				if (success) {
				long end = System.currentTimeMillis()-start;
//				System.err.println("time taken total "+end);
				log.info("time taken for user influence indexing total "+end);
				responseMap.put("status", "success");
				response = mapper.writeValueAsString(responseMap);
				}
							
			} catch (Exception ex) {
				log.error(ex.getMessage());
//				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
						+ "}";
			}
			routingContext.response().end(response);

		}
		
	 /**
	   * use to delete tweets by Date 
	   * 
	   * @param routingContext
	   */
	
	  public void deleteInfluenceBlocking(RoutingContext routingContext) {
	    Map<String, Object> responseMap = new HashMap<String, Object>();
	    String response;
	    
	    ObjectMapper mapper = new ObjectMapper();
	    String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
	    String userScreenName = (routingContext.request().getParam("userScreenName") == null) ? "": routingContext.request().getParam("userScreenName").toLowerCase();
		String influenceScreenName = (routingContext.request().getParam("influenceScreenName") == null) ? "": routingContext.request().getParam("influenceScreenName").toLowerCase();
		String getuserObject[] = {"userScreenName"};
		ArrayList<String> commonRelation = new ArrayList<String>();
		ArrayList<String> nonCommonRelation = new ArrayList<String>();
		ArrayList<String> friendsWhoFollowMe = new ArrayList<String>();
		ArrayList<String> friendsNotFollowMe = new ArrayList<String>();
		try {
			
			
		ArrayList<Map<String, Object>> documents = this.elasticsearch.searchUserRelationDocuments(elasticsearch.getESInstance(esHost, esPort),indexEncription, getuserObject, userScreenName,0, 1);
	    if (documents.size() >= 1 ) {
	    	for (String field : documents.get(0).keySet()) {
				if (field.equalsIgnoreCase(userScreenName+influenceScreenName+"FollowerRelation") || field.equalsIgnoreCase(userScreenName+influenceScreenName+"NonFollowerRelation")) {
					commonRelation = (ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"FollowerRelation");
			    	nonCommonRelation = (ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"NonFollowerRelation");
					
					
						}
				if (field.equalsIgnoreCase(userScreenName+influenceScreenName+"FriendsWhoFollowMe") || field.equalsIgnoreCase(userScreenName+influenceScreenName+"FriendsNotFollowMe")) {
					friendsWhoFollowMe = (ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"FriendsWhoFollowMe");
					friendsNotFollowMe = (ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"FriendsNotFollowMe");
					
					
						}
				
					}

			
				}
	    	

	    // deleting influencer type data for common and then type
	    if (commonRelation.size() >= 1) {
	    	 deleteFromES(indexEncription, userScreenName+influenceScreenName+"commonfollowers", commonRelation); 
		}
	    
	    boolean commonFollowersType = this.elasticsearch.deleteOnlyMaping(this.elasticsearch.getESInstance(this.esHost, this.esPort), indexEncription,userScreenName+influenceScreenName+"commonfollowers");
		
	    
	   // deleting influencer type data for NonCommon and then type
	    if (nonCommonRelation.size() >= 1) {
	    	deleteFromES(indexEncription, userScreenName+influenceScreenName+"noncommonfollowers", nonCommonRelation);

		}
	    
	    boolean nonCommonFollowersType = this.elasticsearch.deleteOnlyMaping(this.elasticsearch.getESInstance(this.esHost, this.esPort), indexEncription,userScreenName+influenceScreenName+"noncommonfollowers");
	   	 
	 // deleting influencer type data for friendswhoFollowMe and then type
	    if (friendsWhoFollowMe.size() >= 1) {
	    	 deleteFromES(indexEncription, userScreenName+influenceScreenName+"friendswhofollow", friendsWhoFollowMe); 
		}
	    
	    boolean friendsWhoFollowType = this.elasticsearch.deleteOnlyMaping(this.elasticsearch.getESInstance(this.esHost, this.esPort), indexEncription,userScreenName+influenceScreenName+"friendswhofollow");
		
	    
	   // deleting influencer type data for friendsWhoNotFollowMe and then type
	    if (friendsNotFollowMe.size() >= 1) {
	    	deleteFromES(indexEncription, userScreenName+influenceScreenName+"friendsnotfollow", friendsNotFollowMe);

		}
	    
	    boolean friendsNotFollowType = this.elasticsearch.deleteOnlyMaping(this.elasticsearch.getESInstance(this.esHost, this.esPort), indexEncription,userScreenName+influenceScreenName+"friendsnotfollow");

	    // deleting timelinetweet type
	    boolean timeLineTweetType = this.elasticsearch.deleteOnlyMaping(this.elasticsearch.getESInstance(this.esHost, this.esPort), indexEncription,influenceScreenName+"-timelinetweets");

	    
	    boolean success = false;
	    
	   if (commonFollowersType && nonCommonFollowersType && friendsWhoFollowType && friendsNotFollowType && timeLineTweetType) {
		   
			success = true;
			
		}
	     
	      if (success) {
	    	  responseMap.put("status", "true");
		}
	      else {
	    	  responseMap.put("status", "false");
		}
	      
	      response = mapper.writeValueAsString(responseMap);

	    } catch (Exception e) {
	    	log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

	      response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
	          + "}";
	    }

	    routingContext.response().end(response);

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
	
	public Map<String, ArrayList<String>> updateUserInfluencerRelation(String indexEncription,String credentialScreeName,String influncerScreenName,String userId,ArrayList<String> credentialFollowerIds,ArrayList<String> influenceFollowerIds,ArrayList<String> influenceFriendIds) throws Exception { // Update User

		Map<String, ArrayList<String>> relationIds = new HashMap<String, ArrayList<String>>();
		try {
			// updating  credential user data   with common followers with influence
			ArrayList<String> followersWhoCommon = new ArrayList<String>();
			ArrayList<String> followersNonCommon = new ArrayList<String>();
			for (int i = 0; i < influenceFollowerIds.size(); i++) {
				int var = credentialFollowerIds.contains(influenceFollowerIds.get(i)) ? 1 : 0;
				if (var == 1) {
					followersWhoCommon.add(influenceFollowerIds.get(i));
				} else {
					followersNonCommon.add(influenceFollowerIds.get(i));
				}
			}
			
			
			// updating  credential user data with influencer friends who follow credentials or not
						ArrayList<String> friendsWhoFollowMe = new ArrayList<String>();
						ArrayList<String> friendsWhoNotFollowMe = new ArrayList<String>();
						
						for (int i = 0; i < influenceFriendIds.size(); i++) {
							
							int var = credentialFollowerIds.contains(influenceFriendIds.get(i)) ? 1 : 0;
							
							if (var == 1) {
								friendsWhoFollowMe.add(influenceFriendIds.get(i));
							} else {
								friendsWhoNotFollowMe.add(influenceFriendIds.get(i));
							}
						}
			
			// checking id already exist then deleting previous data
			String[]  userid= {userId};
			TransportClient client1 = elasticsearch.getESInstance(this.esHost, this.esPort);
			ArrayList<Map<String, Object>> unfollowed = this.elasticsearch.searchUserDocumentsByIds(client1,indexEncription, "user",userid);
			boolean enterFollowerRelatiions=false;
			boolean enterFriendRelatiions=false;
			ArrayList<String> esCommonFollower = new ArrayList<String>();
			ArrayList<String> esNonCommonFollower = new ArrayList<String>();
			
			//to get data influencer friends who follow credential user or not
			ArrayList<String> esInfluencerFriendsWhoFollowMe = new ArrayList<String>();
			ArrayList<String> esInfluencerFriendsNotFollowMe = new ArrayList<String>();
			
			if (unfollowed.size() > 0 ) {
				
				for (String field : unfollowed.get(0).keySet()) {
			if (field.equalsIgnoreCase(credentialScreeName+influncerScreenName+"FollowerRelation") || field.equalsIgnoreCase(credentialScreeName+influncerScreenName+"NonFollowerRelation")) {
				esCommonFollower = (ArrayList<String>) unfollowed.get(0).get(credentialScreeName+influncerScreenName+"FollowerRelation");
				esNonCommonFollower = (ArrayList<String>) unfollowed.get(0).get(credentialScreeName+influncerScreenName+"NonFollowerRelation");
				enterFollowerRelatiions=true;
				
					}
			
			//gettting data for friends who follow me or not from Es
			if (field.equalsIgnoreCase(credentialScreeName+influncerScreenName+"FriendsWhoFollowMe") || field.equalsIgnoreCase(credentialScreeName+influncerScreenName+"FriendsNotFollowMe")) {
				esInfluencerFriendsWhoFollowMe = (ArrayList<String>) unfollowed.get(0).get(credentialScreeName+influncerScreenName+"FriendsWhoFollowMe");
				esInfluencerFriendsNotFollowMe = (ArrayList<String>) unfollowed.get(0).get(credentialScreeName+influncerScreenName+"FriendsNotFollowMe");
				enterFriendRelatiions=true;
				
					}
			
				}
				
			}
			
			// getting those common relation who unfollowed and then deleting from Es so data Synchronized and user get accurate data
			ArrayList<String> commonUnFollowed = new ArrayList<String>();
			ArrayList<String> nonCommonUnFollowed = new ArrayList<String>();
			
			if (enterFollowerRelatiions) {
				// finding common followers who unfollowed

					int check ;
					for (int i = 0; i < esCommonFollower.size(); i++) {
						
						check = followersWhoCommon.contains(esCommonFollower.get(i)) ? 1 : 0;
						if (check == 0) {
							commonUnFollowed.add(esCommonFollower.get(i).toString());						}
					}
					
					//finding common followers who unfollowed
					for (int i = 0; i < esNonCommonFollower.size(); i++) {
						
						check = followersNonCommon.contains(esNonCommonFollower.get(i)) ? 1 : 0;
						if (check == 0) {
							nonCommonUnFollowed.add(esNonCommonFollower.get(i).toString());						}
					}
				
			}
			
			// deleting those who Unfolloed me in mutualRelation type
			
		if (commonUnFollowed.size() >= 1) {
				
				// deleting from mutualRelation
				deleteFromES(indexEncription,credentialScreeName+influncerScreenName+"commonfollowers", commonUnFollowed);
		}
	
		// getting those who Unfolloed me in nonCommonrelation and deleting
		
	if (nonCommonUnFollowed.size() >= 1) {
			// deleting from mutualRelation
			deleteFromES(indexEncription,credentialScreeName+influncerScreenName+"noncommonfollowers", nonCommonUnFollowed);
	}
	
	// getting those relation who follow me or not unfollowed and then deleting from Es so data Synchronized and user get accurate data
				ArrayList<String> friendsWhoFollowMeUnFollowed = new ArrayList<String>();
				ArrayList<String> friendsNotFollowMeUnFollowed = new ArrayList<String>();
				
				if (enterFriendRelatiions) {
					// finding influencer friends who follow me and then unfollowed

						int check ;
						for (int i = 0; i < esInfluencerFriendsWhoFollowMe.size(); i++) {
							
							check = friendsWhoFollowMe.contains(esInfluencerFriendsWhoFollowMe.get(i)) ? 1 : 0;
							if (check == 0) {
								friendsWhoFollowMeUnFollowed.add(esInfluencerFriendsWhoFollowMe.get(i).toString());						}
						}
						
						//finding influencer friends who Not follow me and then unfollowed influencer
						for (int i = 0; i < esInfluencerFriendsNotFollowMe.size(); i++) {
							
							check = friendsWhoNotFollowMe.contains(esInfluencerFriendsNotFollowMe.get(i)) ? 1 : 0;
							if (check == 0) {
								friendsNotFollowMeUnFollowed.add(esInfluencerFriendsNotFollowMe.get(i).toString());						}
						}
					
				}
				
				// deleting those who Unfolloed me in mutualRelation type
				
			if (friendsWhoFollowMeUnFollowed.size() >= 1) {
					
					// deleting from mutualRelation
					deleteFromES(indexEncription,credentialScreeName+influncerScreenName+"friendswhofollow", friendsWhoFollowMeUnFollowed);
			}
		
			// getting those who Unfolloed me in nonCommonrelation and deleting
			
		if (friendsNotFollowMeUnFollowed.size() >= 1) {
				// deleting from mutualRelation
				deleteFromES(indexEncription,credentialScreeName+influncerScreenName+"friendsnotfollow", friendsNotFollowMeUnFollowed);
		}
	
			relationIds.put("commonRelation",followersWhoCommon);
			relationIds.put("nonCommonRelation",followersNonCommon);
			relationIds.put("friendsWhoFollowMe",friendsWhoFollowMe);
			relationIds.put("friendsNotFollowMe",friendsWhoNotFollowMe);
			
//			Collections.sort(followersNonCommon.subList(0, followersNonCommon.size()));
//			Collections.sort(followersWhoCommon.subList(0, followersWhoCommon.size()));
			
			Map<String, Object> updatefollowerRelation = new HashMap<String, Object>();
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"FollowerRelation",followersWhoCommon);
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"NonFollowerRelation",followersNonCommon);
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"FriendsWhoFollowMe",friendsWhoFollowMe);
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"FriendsNotFollowMe",friendsWhoNotFollowMe);
			TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
			client.prepareUpdate(indexEncription,"user",userId).setDoc(updatefollowerRelation).setUpsert(updatefollowerRelation).setRefresh(true).execute().actionGet();
			client.close();
		
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		return relationIds;
	}
	
	
	/**
	 * use to index tweets for given keyword
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexTweetsBlocking(RoutingContext routingContext) {
		String response = "";
		Map<String, Object> responseMap = new HashMap<String, Object>();


		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> credentialsMap = new HashMap<String, Object>();
		String keywordsJson = (routingContext.request().getParam("keywords") == null) ? "['cricket', 'football']": routingContext.request().getParam("keywords");
		String userId_gid = (routingContext.request().getParam("userId_gid") == null) ? "": routingContext.request().getParam("userId_gid");

		long start = System.currentTimeMillis();
		boolean gotResult = true;
		String type = this.type;
		String indexEncription = "tweets_"+userId_gid;
		
		try {
			
			Map<String, Object> userInfo = new HashMap<>();
			userInfo = mysql.getAllInfo(dbName, dbUser, dbPassword, userId_gid);
			
			
			String[] keywords = mapper.readValue(keywordsJson, String[].class);
			
			List<String> listkeywords = new ArrayList<String>(Arrays.asList(keywords));
			
			if (listkeywords.size() > 15) {
    			listkeywords = UtilFunctions.pickNRandom(listkeywords, 15);
    			System.err.println("size "+listkeywords.size());
			}
			
			String queryKeywords = UtilFunctions.listToString(listkeywords);
			
			System.err.println("user "+indexEncription+" query Keywords: "+queryKeywords);
			
			log.info("user "+indexEncription+" query Keywords: "+queryKeywords );
			
			ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String,Object>>();
			if (keywords.length == 0 || indexEncription.isEmpty()) {
				response = "correctly pass keywords or credentials ";
				log.warn("correctly pass keywords or credentials");
			} else {
				
				credentialsMap = mysql.getAuthKeys(dbName,dbUser,dbPassword);
				
		    	mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentialsMap.get("consumerKey").toString());
					
				tweets = this.searchTweets(this.getTwitterInstance((String) credentialsMap.get("consumerKey"),(String)credentialsMap.get("consumerSecret"),(String) credentialsMap.get("accessToken"),(String) credentialsMap
					.get("accessTokenSecret")),	queryKeywords,dbName,dbUser,dbPassword);
			
					
					LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
					for (int i = 0; i < tweets.size(); i += bulkSize) {
						ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(
								tweets.subList(i,
										Math.min(i + bulkSize, tweets.size())));
						bulks.add(bulk);
					}
					
					for (ArrayList<Map<String, Object>> tweetsList : bulks) {
						
						this.indexInES(indexEncription,type,tweetsList);
					}
	
				}
				
			if (tweets.size()==0) {
				gotResult=false;
			}
			if (gotResult) {
				responseMap.put("status", "true");
				responseMap.put("size",tweets.size());
				System.err.println("result "+responseMap);
				log.info("result "+responseMap);
				mysql.updateLogs(dbName, dbUser, dbPassword, userInfo);

			}
			else {
				responseMap.put("status","false");
				System.err.println("result "+responseMap);
				log.info("result "+responseMap);
			}
			
			log.info("time taken total for Index Tweets "+ (System.currentTimeMillis()-start));
			response = mapper.writeValueAsString(responseMap);
			System.err.println("service response: "+response);
			log.info("service response: "+response);
			
		} catch (Exception ex) {
			response = 	"{\"status\" : \"error\", \"msg\" :" + ex.getMessage()+ "}";
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		routingContext.response().end(response);

	}
	
	/**
	 * use to index tweets for given keyword
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexUserTimeLineBlocking(RoutingContext routingContext) {
		String response = "";
		Map<String, Object> responseMap = new HashMap<String, Object>();

		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> credentialsMap = new HashMap<String, Object>();
		String indexEncription = (routingContext.request().getParam("indexEncription") == null) ? "": routingContext.request().getParam("indexEncription");
		String screenName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName");
		String limit = (routingContext.request().getParam("limit") == null) ? "3000": routingContext.request().getParam("limit");
		int limittweets = Integer.parseInt(limit);
		long start = System.currentTimeMillis();
		boolean gotResult = true;

		String newTimeLineType = screenName.toLowerCase().trim()+"-timelinetweets";
		
		try {
			
			log.info("user "+indexEncription+" screenName: "+screenName );
			ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String,Object>>();
			if (screenName.isEmpty() || indexEncription.isEmpty()) {
				response = "correctly pass ScreenName or IndexEncription";
				log.warn("correctly pass ScreenName or IndexEncription");
			} else {
				
				credentialsMap = mysql.getAuthKeys(dbName,dbUser,dbPassword);
				
		    	mysql.updateTimeStamp(dbName,dbUser,dbPassword, credentialsMap.get("consumerKey").toString());
					
				tweets = this.getUserTimeLine(this.getTwitterInstance((String) credentialsMap.get("consumerKey"),(String)credentialsMap.get("consumerSecret"),(String) credentialsMap.get("accessToken"),(String) credentialsMap
					.get("accessTokenSecret")),	screenName,limittweets,dbName,dbUser,dbPassword);
			
					
					LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
					for (int i = 0; i < tweets.size(); i += bulkSize) {
						ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(
								tweets.subList(i,
										Math.min(i + bulkSize, tweets.size())));
						bulks.add(bulk);
					}
					
					for (ArrayList<Map<String, Object>> tweetsList : bulks) {
						
						this.indexInES(indexEncription,newTimeLineType,tweetsList);
					}
	
				}
				
			if (tweets.size()==0) {
				gotResult=false;
			}
			if (gotResult) {
				responseMap.put("status", "true");
				responseMap.put("size",tweets.size());
				System.err.println("result "+responseMap);
				log.info("result "+responseMap);
//				mysql.updateLogs(dbName, dbUser, dbPassword, userInfo);

			}
			else {
				responseMap.put("status","false");
				System.err.println("result "+responseMap);
				log.info("result "+responseMap);
			}
			
			log.info("time taken total for Index Tweets "+ (System.currentTimeMillis()-start));
			response = mapper.writeValueAsString(responseMap);
			System.err.println("service response: "+response);
			log.info("service response: "+response);
			
		} catch (Exception ex) {
			response = 	"{\"status\" : \"error\", \"msg\" :" + ex.getMessage()+ "}";
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		routingContext.response().end(response);

	}
	
	/**
	 * use to mute by using screenName
	 * 
	 * @param routingContext
	 * @param credentials
	 * @param ScreenName
	 *            return name of those on which action has been taken
	 */
		public void muteRouteBlocking(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String friendsListJson = (routingContext.request().getParam("screenNames") == null) ? "[]" : routingContext.request().getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {
			};
			HashMap<String, String> credentials = mapper.readValue(	credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> friendsListType = new TypeReference<ArrayList<String>>() {	};
			ArrayList<String> friendsList = mapper.readValue(friendsListJson,friendsListType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.muteUser(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),credentials.get("accessToken"),credentials.get("accessTokenSecret")), friendsList);
			responseMap.put("muted", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			log.error(ex.getMessage());
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}
	
	public String listToString(List<String> terms){
		String keyword = "";
		int count =0;
		for (int i = 0; i < terms.size(); i++) {
			
			if ( count == terms.size()-1 ) {

				keyword = keyword +	"("+terms.get(i)+")";
				}
			else {
				keyword = keyword +	"("+terms.get(i)+") OR ";
			}
	
			count++;
	
		} 
	
		return keyword.toString();
	}
	
/**
	 * use to index tweets in ES
	 * 
	 * @param tweets
 * @throws Exception 
	 */
	public void indexInES(String indexName,String type,ArrayList<Map<String, Object>> tweets) throws Exception {
		TransportClient client = this.elasticsearch.getESInstance(this.esHost, this.esPort);
		
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Map<String, Object> tweet : tweets) {
//			System.out.println(tweet);
			bulkRequestBuilder.add(client.prepareUpdate(indexName,type,tweet.get("id").toString()).setDoc(tweet)
					.setUpsert(tweet));

		}
		bulkRequestBuilder.setRefresh(true).execute().actionGet();
		

		client.close();
	}
	
	
		/**
		 * use to get userInfo
		 * 
		 * @param routingContext
		 */
		public void userInfoRouteBlocking(RoutingContext routingContext) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			String response;
			ObjectMapper mapper = new ObjectMapper();
			
			String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
			String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");

			
			try {
	
				TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String,Object>>() {	};
				HashMap<String, Object> credentials = mapper.readValue(credentialsjson, typeRef);
				ArrayList<Map<String, Object>> documents=null;
				documents = this.userInfo(this.getTwitterInstance(credentials.get("consumerKey").toString(),credentials.get("consumerSecret").toString(),credentials.get("accessToken").toString(),credentials.get("accessTokenSecret").toString()),screenName);
				
				

				if (documents.size()!=0) {
				responseMap.put("userInfo", documents);
				responseMap.put("size", documents.size());
				
			}
			
			response = mapper.writeValueAsString(responseMap);

			} catch (Exception e) {
				log.error(e.getMessage());
//				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
						+ "}";
			}

			routingContext.response().end(response);

		}
		
		/**
		 * use to unfollow by id
		 * 
		 * @param routingContext
		 * @param credentials
		 * @param userIds
		 *            can't follow more than 1000 user in one day, total 5000 users
		 *            can be followed by a account
		 */
			public void unfollowRouteBlocking(RoutingContext routingContext) {
			ObjectMapper mapper = new ObjectMapper();
			HashMap<String, Object> responseMap = new HashMap<String, Object>();
			String response;
			String userIds = (routingContext.request().getParam("screenNames") == null) ? ""
					: routingContext.request().getParam("screenNames");
			String credentialsJson = (routingContext.request().getParam(
					"credentials") == null) ? "" : routingContext.request()
					.getParam("credentials");
			try {
				TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {	};
				HashMap<String, String> credentials = mapper.readValue(credentialsJson, credentialsType);
				TypeReference<ArrayList<String>> followIdsType = new TypeReference<ArrayList<String>>() {};
				ArrayList<String> followIds = mapper.readValue(userIds,followIdsType);
				ArrayList<String> FreindshipResponse = null;
				FreindshipResponse = this.destroyFriendShip(this.getTwitterInstance(credentials.get("consumerKey"),
								credentials.get("consumerSecret"),
								credentials.get("accessToken"),
								credentials.get("accessTokenSecret")), followIds);
				responseMap.put("unfollowing", FreindshipResponse);
				response = mapper.writeValueAsString(responseMap);
			} catch (Exception ex) {
				log.error(ex.getMessage());
//				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
						+ "}";
			}
			routingContext.response().end(response);
		}

		/**
		 * use to get just userInfo not indexing in elasticsearch
		 * 
		 * @param routingContext
		 */
		public void indexUserInfoRouteBlocking(RoutingContext routingContext) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			String response;
			ObjectMapper mapper = new ObjectMapper();
			
			String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
			String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");
			
			try {
	
				TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String,Object>>() {	};
				HashMap<String, Object> credentials = mapper.readValue(credentialsjson, typeRef);
				ArrayList<Map<String, Object>> documents=null;
				documents = this.twitter4jApi.getStickyInfo(this.getTwitterInstance(credentials.get("consumerKey").toString(),credentials.get("consumerSecret").toString(),credentials.get("accessToken").toString(),credentials.get("accessTokenSecret").toString()),screenName);

				if (documents.size()!=0) {	
				responseMap.put("documents", documents);
				responseMap.put("size", documents.size());
				
			}
			
			response = mapper.writeValueAsString(responseMap);

			} catch (Exception e) {
				log.error(e.getMessage());

				response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
						+ "}";
			}

			routingContext.response().end(response);

		}

		
		/**
		 * use to get followerslist
		 * 
		 * @param twitter
		 * @return followersList
		 * @throws TwitterException
		 * @throws SQLException 
		 * @throws InterruptedException 
		 * @throws ClassNotFoundException 

		 * @throws Exception
		 */
		public ArrayList<Map<String, Object>> getUsersInfoByIds(String dbName,String DbUser,String DbPassword,Twitter twitter,long []influenceFollowerIds) throws IllegalStateException,TwitterException, ClassNotFoundException, InterruptedException, SQLException {

			return twitter4jApi.getUsersInfoByIds(dbName,DbUser,DbPassword,twitter,influenceFollowerIds);
		}
		
	/**
		 * use to get tweets
		 * 
		 * @param twitter
		 * @param query
		 * @return list of tweets
		 * @throws TwitterException
		 * @throws Exception
		 */

		public ArrayList<Map<String, Object>> userInfo(Twitter twitter,String screenName)throws Exception {
			ArrayList<Map<String, Object>> userInfo = twitter4jApi.getUserInfo(twitter,screenName);
			return userInfo;

		}

	


	/**
	 * use to get tweets
	 * 
	 * @param twitter
	 * @param query
	 * @return list of tweets
	 * @throws TwitterException
	 * @throws Exception
	 */

	public ArrayList<Map<String, Object>> searchTweets(Twitter twitter,
			String keyword,String dbName,String DbUser,String DbPass) throws Exception {
		ArrayList<Map<String, Object>> tweets = twitter4jApi.search(twitter,keyword,dbName,DbUser,DbPass);
		return tweets;

	}
	/**
	 * use to get tweets
	 * 
	 * @param twitter
	 * @param query
	 * @return list of tweets
	 * @throws TwitterException
	 * @throws Exception
	 */

	public ArrayList<Map<String, Object>> getUserTimeLine(Twitter twitter,
			String ScreenName,int limit,String dbName,String DbUser,String DbPass) throws Exception {
		ArrayList<Map<String, Object>> tweets = twitter4jApi.getuserTimeLine(twitter,ScreenName,limit,dbName,DbUser,DbPass);
		return tweets;

	}
	
	/**
	 * use to Mute User
	 * 
	 * 
	 * @param credentials
	 * @param screenName
	 * @return name of those on which actio has benn taken
	 */
		public ArrayList<String> muteUser(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException {

		return twitter4jApi.muteUser(twitter, ScreenName);
	}
	
		/**
		 * use to destroy friendship
		 * 
		 * @param twitter
		 * @param user
		 *            Screen Name
		 * @return friended data about user
		 * @throws TwitterException
		 * @throws Exception
		 */
		public ArrayList<String> destroyFriendShip(Twitter twitter,
				ArrayList<String> ScreenName) throws TwitterException, Exception {

			return twitter4jApi.destroyFriendship(twitter, ScreenName);
		}
		
	/**
	 * use to get followerIds
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public ArrayList<String> getFollowerIds(String dbName,String dbUser,String dbPass,Twitter twitter, String screenName) throws TwitterException {

		return twitter4jApi.getFollowerIds(dbName,dbUser,dbPassword,twitter, screenName);
	}
	
	/**
	 * use to get friendIds
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public ArrayList<String> getFriendIds(String dbName,String dbUser,String dbPass,Twitter twitter, String screenName) throws TwitterException {

		return twitter4jApi.getFriendIds(dbName,dbUser,dbPassword,twitter, screenName);
	}
	
	/**
	 * use to get twitter instance
	 * 
	 * @param consumerKey
	 * @param consumerSecret
	 * @param accessToken
	 * @param accessTokenSecret
	 * @return
	 */

	public Twitter getTwitterInstance(String consumerKey,String consumerSecret, String accessToken, String accessTokenSecret) throws TwitterException {
		return twitter4jApi.getTwitterInstance(consumerKey, consumerSecret,
				accessToken, accessTokenSecret);
	}
}
