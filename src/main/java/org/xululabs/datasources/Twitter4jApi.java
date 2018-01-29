package org.xululabs.datasources;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xululabs.commands.TtafResponse;

import twitter4j.IDs;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter4jApi {
	private static Logger log = LogManager.getRootLogger();
	MysqlApi mysql = new MysqlApi();
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
			System.out.println("issue in the Keys "+consumerKey);
		}

		return twitter;
	}

	/**
     * use to search in twitter for given keyword
     * @param twitter
     * @param keyword
     * @return
     * @throws Exception
     */
	
	public ArrayList<Map<String, Object>> search(Twitter twitter, String keyword,String dbName,String DbUser,String DbPass)
			throws Exception {
		int searchResultCount = 0;
		int totalcalls=0;
		int deadConditions = 0;
		long lowestTweetId = Long.MAX_VALUE;
		boolean enterDead = false;
		Map<String, Object> consumerKey = new HashMap<String, Object>();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); 
		Date date = new Date();
		int remApiLimits = 180;
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		Query query = new Query(keyword);
		query.setCount(100);
		query.since(dateFormat.format(date));
		query.setLang("en");
		QueryResult queryResult;
		do {
	
			try {
				
				
				if (enterDead) {
					System.err.println("changing Auth keys ");
					consumerKey.clear();
					consumerKey = mysql.getAuthKeys(dbName,DbUser,DbPass);
					mysql.updateTimeStamp(dbName,DbUser,DbPass, consumerKey.get("consumerKey").toString());
					twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
					enterDead = false;
				}
				
				Map<String, RateLimitStatus> rateLimitStatusAppi = twitter.getRateLimitStatus("application");
				RateLimitStatus	AppiRateLimit = rateLimitStatusAppi.get("/application/rate_limit_status");
				remApiLimits = AppiRateLimit.getRemaining();
				
				if (remApiLimits < 20) {
					
					System.err.println("deadlock condition getting new auth keys ");
				log.info("deadlock condition getting new auth keys "+"limit "+remApiLimits);

					System.err.println("limit "+remApiLimits);
					
					if (remApiLimits < 15 ) {
						
						System.err.println("limits exceeding going to sleep ..!");
						log.info("limits exceeding going to sleep ..!");
						Thread.sleep(900000);
						System.err.println("wokeUp from sleep ..!");
						log.info("wokeUp from sleep ..!");
					}
					deadConditions = 0;
					twitter = null;
					enterDead = true;
					continue;	
				}
				
				queryResult = twitter.search(query);
				totalcalls++;
				searchResultCount = queryResult.getTweets().size();
				
				for (Status tweet : queryResult.getTweets()) {
					Map<String, Object> tweetInfo = new HashMap<String, Object>();
					tweetInfo.put("id", tweet.getId());
					tweetInfo.put("tweet", tweet.getText());
					tweetInfo.put("screenName", tweet.getUser().getScreenName());
					tweetInfo.put("userId", tweet.getUser().getId());
					tweetInfo.put("name", tweet.getUser().getName());
					tweetInfo.put("retweetCount", tweet.getRetweetCount());
					double friendsCount = tweet.getUser().getFriendsCount();
					double followersCount = tweet.getUser().getFollowersCount();
					double ratio = 0;
					
					if (friendsCount!=0) {
						 ratio = (followersCount/friendsCount);
					}
					
					tweetInfo.put("ratio", ratio);
					tweetInfo.put("followersCount", tweet.getUser().getFollowersCount());
					tweetInfo.put("friendsCount", tweet.getUser().getFriendsCount());
					tweetInfo.put("user_image", tweet.getUser().getProfileImageURL());
					tweetInfo.put("description", tweet.getUser().getDescription());
					tweetInfo.put("user_location", tweet.getUser().getLocation());
					tweetInfo.put("tweetsCount", tweet.getUser().getStatusesCount());
					tweetInfo.put("tweet_location", tweet.getGeoLocation());
					tweetInfo.put("time",tweet.getCreatedAt().getTime());
					String expandedUrl = "";
					List<String> urls = new ArrayList<String>();

					for (URLEntity urle : tweet.getURLEntities()) {
						 expandedUrl = urle.getExpandedURL();
						 urls.add(urle.getExpandedURL());
						break;
                	 } 

					tweetInfo.put("externalUrl",expandedUrl);
					tweetInfo.put("url",urls);		
					
					tweetInfo.put("date",new SimpleDateFormat("yyyy-MM-dd").format(tweet.getCreatedAt()).toString());
					tweetInfo.put("timeZone", tweet.getUser().getUtcOffset());
					tweets.add(tweetInfo);

					if (tweet.getId() < lowestTweetId) {
						lowestTweetId = tweet.getId();
						query.setMaxId(lowestTweetId);
					}

				}
				
				System.err.println("call No: "+totalcalls+" tweets "+queryResult.getTweets().size()+" remaining limit "+remApiLimits);
				log.info("call No: "+totalcalls+" tweets "+queryResult.getTweets().size()+" remaining limit "+remApiLimits);
				if (tweets.size() >= 10000 || totalcalls >= 115) {
					if (totalcalls ==105 && tweets.size()==0) {
					Map<String, Object> tweetInfo = new HashMap<String, Object>();
					tweetInfo.put("message", "not getting data for terms ");
					tweets.add(tweetInfo);
					}
					break;
				}
				
			} catch (TwitterException e) {
				
				System.err.println(e.getMessage());
				twitter = null;
				Map<String, Object> tweetInfo = new HashMap<String, Object>();
				tweetInfo.put("message", e.getMessage());
				tweets.add(tweetInfo);
				break;
			}
			System.err.println("tweets collected "+ tweets.size());
			
		} while (true);
		
		return tweets;

	}
	

	/**
     * use to get userTimeLine in twitter for given User
     * @param twitter
     * @param User
     * @return
     * @throws Exception
     */
	
	public ArrayList<Map<String, Object>> getuserTimeLine(Twitter twitter, String screenName,int limit,String dbName,String DbUser,String DbPass)
			throws Exception {
		Map<String, Object> consumerKey = new HashMap<String,Object>();
		ArrayList<Map<String, Object>> userTimeLine = new ArrayList<Map<String, Object>>();
		
			int deadCondition = 0;
			
			List ListTweetes = new ArrayList();
			int remApiLimits = 900;
			int pageNo = 1 ;
			do {
			
				try {

					if (remApiLimits < 20) {
						
						consumerKey = mysql.getAuthKeys(dbName,DbUser,DbPass);
						
						mysql.updateTimeStamp(dbName,DbUser,DbPass, consumerKey.get("consumerKey").toString());
						
						twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
						deadCondition = 0;	
					}
					
					Paging paging = new Paging(pageNo++, 200);
					int size = ListTweetes.size(); 
					ListTweetes.addAll(twitter.getUserTimeline(screenName,paging));

					List<Status> userTimeline = twitter.getUserTimeline(screenName,paging);
					
					remApiLimits = twitter.getUserTimeline().getRateLimitStatus().getRemaining();
					
					for (Status tweet: userTimeline) {
						Map<String, Object> tweetInfo = new HashMap<String, Object>();
						tweetInfo.put("id", tweet.getId());
    					tweetInfo.put("tweet", tweet.getText());
    					tweetInfo.put("screenName", tweet.getUser().getScreenName());
    					tweetInfo.put("userId", tweet.getUser().getId());
    					tweetInfo.put("name", tweet.getUser().getName());
    					tweetInfo.put("retweetCount", tweet.getRetweetCount());
    					double friendsCount = tweet.getUser().getFriendsCount();
    					double followersCount = tweet.getUser().getFollowersCount();
    					double ratio = 0;
    					if (friendsCount!=0) {
    						 ratio = (followersCount/friendsCount);
    					}
    					tweetInfo.put("ratio", ratio);
    					tweetInfo.put("followersCount", tweet.getUser().getFollowersCount());
    					tweetInfo.put("friendsCount", tweet.getUser().getFriendsCount());
    					tweetInfo.put("user_image", tweet.getUser().getProfileImageURL());
    					tweetInfo.put("description", tweet.getUser().getDescription());
    					tweetInfo.put("user_location", tweet.getUser().getLocation());
    					tweetInfo.put("tweet_location", tweet.getGeoLocation());
    					tweetInfo.put("time",tweet.getCreatedAt().getTime());
    					String expandedUrl = "";
    					List<String> urls = new ArrayList<String>();
    					/*for (URLEntity urle : tweet.getURLEntities()) {
    						 expandedUrl = urle.getExpandedURL();
    						 urls.add(urle.getExpandedURL());
    						break;
                    	 	} 
    						tweetInfo.put("externalUrl",expandedUrl);
    						tweetInfo.put("url",urls);*/

    					tweetInfo.put("date",new SimpleDateFormat("yyyy-MM-dd").format(tweet.getCreatedAt()).toString());
    					tweetInfo.put("timeZone", tweet.getUser().getUtcOffset());
    					userTimeLine.add(tweetInfo);
					}
	
					if (userTimeLine.size() > limit || ListTweetes.size() == size) {
						break;
					}
				}
		
					
				 catch (TwitterException e) {
					deadCondition++;
					
					if (e.getErrorCode() == 88) {
						
						System.err.println("deadlock condition getting new auth keys ");
						
						consumerKey = mysql.getAuthKeys(dbName,DbUser,DbPass);
						
						mysql.updateTimeStamp(dbName,DbUser,DbPass, consumerKey.get("consumerKey").toString());
						
						twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
					
						if (deadCondition == 4) {
						Thread.sleep(800000);
						deadCondition = 0;
						}
					}
				}
			}
		
			while(true); 
			
		return userTimeLine;
	}

	
	
	/**
	   * use to get info about user
	   * 
	   * @param twitter
	   * @param ScreenName
	   * @return blocked, info of the person
	   * @throws TwitterException
	   */
	  public ArrayList<Map<String, Object>> getUserInfo(Twitter twitter,String screenName) throws TwitterException {

	    ArrayList<Map<String, Object>> userInfo = new ArrayList<Map<String, Object>>();
	    Map<String, Object> user = null;  
	    try {          
	    	if (screenName.isEmpty()) {
				screenName = twitter.getScreenName();
			}
	    
	      User followersCount = twitter.showUser(screenName);
	      user = new HashMap<String, Object>();
	      user.put("userScreenName", followersCount.getScreenName());
	      user.put("friendsCount",followersCount.getFriendsCount());
	      user.put("followersCount",followersCount.getFollowersCount());	
	      double friendsCount = followersCount.getFriendsCount();
	      double followersCounts = followersCount.getFollowersCount();
	      double ratio = 0;
			if (friendsCount!=0) {
				 ratio = (followersCounts/friendsCount);
			}
			
		  user.put("ratio", ratio);
	      user.put("id", followersCount.getId());
	      user.put("user_image", followersCount.getProfileImageURL());
	      user.put("description", followersCount.getDescription());
	      user.put("tweetsCount", followersCount.getStatusesCount());
	      user.put("user_location", followersCount.getLocation());
	      user.put("timeZone",followersCount.getUtcOffset());
	      user.put("time",followersCount.getCreatedAt().getTime());
	      userInfo.add(user);

	    } catch (Exception e) {

	      e.printStackTrace();
	      log.error(e.getMessage());
//	      log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());
	    }

	    return userInfo;
	  }
	  
		/**
	   * use to get info about user
	   * 
	   * @param twitter
	   * @param ScreenName
	   * @return blocked, info of the person
	   * @throws TwitterException
	   */
	  public ArrayList<Map<String, Object>> getStickyInfo(Twitter twitter,String screenName) throws TwitterException {

	    ArrayList<Map<String, Object>> userInfo = new ArrayList<Map<String, Object>>();
	    try {          
	    	if (screenName.isEmpty()) {
				screenName = twitter.getScreenName();
			}
	      Map<String, Object> user = null;      
	      User followersCount = twitter.showUser(screenName);
	      user = new HashMap<String, Object>();
	      user.put("screenName", followersCount.getScreenName());
	      user.put("friendsCount",followersCount.getFriendsCount());
	      user.put("followersCount",followersCount.getFollowersCount());
	      double friendsCount = followersCount.getFriendsCount();
	      double followersCounts = followersCount.getFollowersCount();
	      double ratio = 0;
			if (friendsCount!=0) {
				 ratio = (followersCounts/friendsCount);
			}
			
		  user.put("ratio", ratio);
	      user.put("id", followersCount.getId());
	      user.put("user_image", followersCount.getProfileImageURL());
	      user.put("description", followersCount.getDescription());
	      user.put("tweetsCount", followersCount.getStatusesCount());
	      user.put("user_location", followersCount.getLocation());
	      user.put("timeZone",followersCount.getUtcOffset());
	      user.put("time",followersCount.getCreatedAt().getTime());
	      userInfo.add(user);

	    } catch (Exception e) {

	      e.printStackTrace();
	      log.error(e.getMessage());
//	      log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());
	    }

	    return userInfo;
	  }
	
	/**
	 * use to create friendship
	 * 
	 * @param twitter
	 * @param ScreenName
	 * @return friended, info of the person
	 * @throws TwitterException
	 */
	public ArrayList<String> createFriendship(Twitter twitter,ArrayList<String> ScreenName) throws TwitterException {

		ArrayList<String> tweets = new ArrayList<String>();
		
		
		try {
			
			for (String  user: ScreenName) {
				
				User tweet = twitter.createFriendship(user);
				if (!tweet.getScreenName().isEmpty()) {
				tweets.add(tweet.getScreenName());
			
				}
			} 

		} catch (Exception e) {
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());
			log.error(e.getMessage());
			e.printStackTrace();
		}

		return tweets;
	}
	/**
	 * use to destroy friendship
	 * 
	 * @param twitter
	 * @param ScreenName
	 * @return UnFriended info of the person
	 * @throws TwitterException
	 */
	public ArrayList<String> destroyFriendship(Twitter twitter,ArrayList<String> ScreenName) throws TwitterException {

		ArrayList<String> tweets = new ArrayList<String>();
		try {
			
			Map<String, Object> tweetInfo = null;
			for (String  user: ScreenName) {
				User tweet = twitter.destroyFriendship(user);
				if (!tweet.getScreenName().isEmpty()) {
				tweets.add(tweet.getScreenName());	
				} 
			}
			
		} catch (Exception e) {
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());
			log.error(e.getMessage());
			e.printStackTrace();
		}

		return tweets;
	}
	
	/**
	 * use to block user
	 * 
	 * @param twitter
	 * @param ScreenName
	 * @return blocked, info of the person
	 * @throws TwitterException
	 */
	public ArrayList<String> muteUser(Twitter twitter,ArrayList<String> ScreenName) throws TwitterException {

		ArrayList<String> tweets = new ArrayList<String>();
		try {
			
			for (String  user: ScreenName) {
				User tweet = twitter.createMute(user);
				if (!tweet.getScreenName().isEmpty()) {
					tweets.add(user);
				}
				
			} 

		} catch (Exception e) {
//			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());
			log.error(e.getMessage());
			e.printStackTrace();
		}

		return tweets;
	}
	

	
	/**
	 * use to get info by passing ids 
	 * 
	 * @param twitter
	 * @return friendListIds,of the user
	 * @throws TwitterException
	 * @throws InterruptedException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */

	public ArrayList<Map<String, Object>> getUsersInfoByIds(String dbName,String DbUser,String DbPassword,Twitter twitter,long[] influenceFollowerIds) throws TwitterException, InterruptedException, ClassNotFoundException, SQLException {
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		
		int deadCondition = 0;
		Map<String, Object> consumerKey = new HashMap<String,Object>();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {

			int remApiLimits = 900;
			boolean enterDead = false;
			
				LinkedList<long[]> chunks = chunks(influenceFollowerIds, 100);
				
				for (int j = 0; j < chunks.size();j++) {
					
					if (enterDead) {
						consumerKey = mysql.getAuthKeys(dbName,DbUser,DbPassword);
						mysql.updateTimeStamp(dbName,DbUser,DbPassword, consumerKey.get("consumerKey").toString());
						twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(), consumerKey.get("consumerSecret").toString(), consumerKey.get("accessToken").toString(), consumerKey.get("accessTokenSecret").toString());	
						Map<String, RateLimitStatus> rateLimitStatusAppi = twitter.getRateLimitStatus("application");
						RateLimitStatus	AppiRateLimit = rateLimitStatusAppi.get("/application/rate_limit_status");
						remApiLimits = AppiRateLimit.getRemaining();
						enterDead = false;
					}
					
					if (remApiLimits < 20) {
						
						if (remApiLimits < 18 ) {
							
							System.err.println("limits exceeding going to sleep ..!");
							Thread.sleep(900000);
							System.err.println("wokeUp from sleep ..!");
							
						}
						System.err.println("changing auth kesy limit "+remApiLimits);
						twitter = null;
						enterDead =true;
						j--;
						deadCondition = 0;
						continue;
						
					}
					
					ResponseList<User> users = twitter.lookupUsers(chunks.get(j));
					Map<String, Object> tweetInfo = null;
					for (int i=0;i<users.size();i++) {	
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
						tweetInfo.put("time",users.get(i).getCreatedAt());
						tweets.add(tweetInfo);
					}

				}
				

		} catch (TwitterException e) {
			log.error(e.getMessage());

			System.err.println(e.getMessage());
			deadCondition++;
			if (e.getErrorCode() == 88) {
				
				System.err.println("deadlock condition getting new auth keys ");
						
				if (deadCondition == 4) {
					
				Thread.sleep(800000);
				deadCondition = 0;
				
				}
			}	
		}
		return tweets;
	}
	
	
	/**
	 * use to get Ids of followers
	 * 
	 * @param twitter
	 * @return followetrIds,of the user
	 * @throws TwitterException
	 */

	public ArrayList<String> getFollowerIds(String dbName,String dbUser,String dbPassword,Twitter twitter,String screenName) throws TwitterException {

		ArrayList<String> followersIds = new ArrayList<String>();
			if (screenName.isEmpty()) {
				screenName = twitter.getScreenName();
			}
			Map<String, Object> consumerKey = new HashMap<String, Object>();
			int	deadCondition = 0;
		try {
			long cursor = -1;
			IDs followerIDs = null;
			long[] followerIds = null;
			int remApiLimits = 15;
			boolean enteredDead=false;
			do {
				
				try {

					if (remApiLimits < 3) {
						
						if (remApiLimits < 2 ) {
							System.err.println("limits exceeding going to sleep ..!");
							Thread.sleep(900000);
							System.err.println("wokeUp from sleep ..!");
						}
						System.err.println("changing Auth keys limit "+remApiLimits+" keys "+consumerKey);
						consumerKey = mysql.getAuthKeys(dbName,dbUser,dbPassword);
						mysql.updateTimeStamp(dbName,dbUser,dbPassword, consumerKey.get("consumerKey").toString());
						
						twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
						deadCondition = 0;
					}

					followerIDs = twitter.getFollowersIDs(screenName, cursor);
					followerIds  =  followerIDs.getIDs();
					cursor = followerIDs.getNextCursor();
					for (int i = 0; i < followerIds.length; i++) {
						
						followersIds.add(Long.toString(followerIds[i]));
				
					}
					
					cursor = followerIDs.getNextCursor();
					remApiLimits = followerIDs.getRateLimitStatus().getRemaining();

				} catch (TwitterException e) {
					deadCondition++;
					
					if (e.getErrorCode() == 88) {
						
						if (deadCondition == 4) {
							System.err.println("sleeping for 15 minutes !");
							Thread.sleep(800000);
							deadCondition = 0;
						}
						
						System.err.println("deadlock condition getting new auth keys ");
						
					}
				}

			} while ((cursor != 0));	

		} catch (Exception e) {
			
			System.err.println(e.getMessage());
			
		}
		
		return followersIds;
		
	}
	
	/**
	 * use to get Ids of followers
	 * 
	 * @param twitter
	 * @return followetrIds,of the user
	 * @throws TwitterException
	 */

	public ArrayList<String> getFriendIds(String dbName,String dbUser,String dbPassword,Twitter twitter,String screenName) throws TwitterException {

		ArrayList<String> friendsIds = new ArrayList<String>();
			if (screenName.isEmpty()) {
				screenName = twitter.getScreenName();
			}
			Map<String, Object> consumerKey = new HashMap<String, Object>();
			int	deadCondition = 0;
		try {
			long cursor = -1;
			IDs friendIDs = null;
			long[] friendIds = null;
			int remApiLimits = 15;
			boolean enteredDead=false;
			do {
				
				try {

					if (remApiLimits < 3) {
						
						if (remApiLimits < 2 ) {
							System.err.println("limits exceeding going to sleep ..!");
							Thread.sleep(900000);
							System.err.println("wokeUp from sleep ..!");
						}
						
						System.err.println("changing Auth keys limit "+remApiLimits+" keys "+consumerKey);
						consumerKey = mysql.getAuthKeys(dbName,dbUser,dbPassword);
						
						mysql.updateTimeStamp(dbName,dbUser,dbPassword, consumerKey.get("consumerKey").toString());
						
						twitter = this.getTwitterInstance(consumerKey.get("consumerKey").toString(),consumerKey.get("consumerSecret").toString(), consumerKey
								.get("accessToken").toString(),consumerKey.get("accessTokenSecret").toString());
						deadCondition = 0;
					}

					friendIDs = twitter.getFriendsIDs(screenName, cursor);
					friendIds  =  friendIDs.getIDs();
					cursor = friendIDs.getNextCursor();
					for (int i = 0; i < friendIds.length; i++) {
						
						friendsIds.add(Long.toString(friendIds[i]));
				
					}
					
					cursor = friendIDs.getNextCursor();
					remApiLimits = friendIDs.getRateLimitStatus().getRemaining();

				} catch (TwitterException e) {
					deadCondition++;
					
					if (e.getErrorCode() == 88) {
						
						if (deadCondition == 4) {
							System.err.println("sleeping for 15 minutes !");
							Thread.sleep(800000);
							deadCondition = 0;
						}
						
						System.err.println("deadlock condition getting new auth keys ");
						
					}
				}

			} while ((cursor != 0));	

		} catch (Exception e) {
			
			System.err.println(e.getMessage());
			
		}
		
		return friendsIds;
		
	}
	
	public static LinkedList<long[]> chunks(long[] bigList, int n) {
		int partitionSize = n;
		LinkedList<long[]> partitions = new LinkedList<long[]>();
		for (int i = 0; i < bigList.length; i += partitionSize) {
			long[] bulk = Arrays.copyOfRange(bigList, i,
					Math.min(i + partitionSize, bigList.length));
			partitions.add(bulk);
		}

		return partitions;
	}

	

}
