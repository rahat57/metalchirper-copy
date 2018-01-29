package org.xululabs.datasources;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

public class ElasticsearchApi {
	private static Logger log = LogManager.getRootLogger();
	
	UtilFunctions UtilFunctions ;
	String esHost;
	int esPort;
	 public ElasticsearchApi() {
		 
		 this.UtilFunctions =new UtilFunctions();
		 this.esHost = "localhost";
		 this.esPort = 9300;
		 
	 }
	/**
	 * use to get elasticsearch instance
	 * 
	 * @param host
	 * @param port
	 * @return client
	 */
	 public TransportClient getESInstance(String host, int port)
			throws Exception {
		

		 Settings settings = ImmutableSettings.settingsBuilder()   //.put(settingsBuilder.toString())
			        .put("cluster.name", "elasticsearch-metalchirper-v2").build();
		 
			TransportClient client = new TransportClient(settings)
			.addTransportAddress(new InetSocketTransportAddress(host, port));
			
			return client;
	
	}
	 
	// create new index for only user relation if already exist delete it and recreate

	 public boolean	createUserRelationIndex(String indexEncription,String type,LinkedList<long[]> commonRelationChunks,LinkedList<long[]> nonCommonFollowersChunks,LinkedList<long[]> nonCommonFriendsChunks,ArrayList<String> commonRelation,ArrayList<String> followersNonCommon,ArrayList<String> friendsNonCommon){
			
		 boolean relationDocumentscreated = false;
		
		 Map<String, Object> userRelation = new HashMap<String,Object>();


		try {
			
			// deleting existing index and recreating and adding data
			TransportClient client = this.getESInstance(this.esHost, this.esPort);
			
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -1);
			
			String deleteDate = format.format(cal.getTime()).toString();
			
		    String currentDate = format.format(System.currentTimeMillis()).toString();
			
			 boolean IndexExist = this.indexExist(client, indexEncription);
			 
			 if (IndexExist) {
				 
				 this.deleteOldTweetsByDate(client, indexEncription, type, deleteDate);	
			}
			 
			 // for common followers relation value 1
			 userRelation.put("relation", 1);
			 userRelation.put("date", currentDate);
			 
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

			 for (int i = 0; i < commonRelationChunks.size(); i++) {
				 
				 long commanArray[] = commonRelationChunks.get(i);
				 
				for (int j = 0; j < commanArray.length-1; j++) {
					
					String id = Long.toString(commanArray[j]);
					userRelation.put("id", id);
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
							userRelation.put("id", id);
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
							userRelation.put("id", id);	
							bulkRequestBuilder.add(client.prepareUpdate(indexEncription,type,id).setDoc(userRelation).setUpsert(userRelation));
						}
									
						bulkRequestBuilder.setRefresh(true).execute().actionGet();
					} 
			 
				 client.close();
					

					String[] credentialcommonIds = UtilFunctions.getArrayIds(commonRelation);
					String[] credentialNonCommonFollowerIds = UtilFunctions.getArrayIds(followersNonCommon);
					String[] credentialNonCommonFriendsIds =UtilFunctions. getArrayIds(friendsNonCommon);
					
					
			if (this.documentsExist(this.getESInstance(this.esHost, this.esPort),indexEncription, type, credentialcommonIds) && this.documentsExist(this.getESInstance(this.esHost, this.esPort),indexEncription,type, credentialNonCommonFollowerIds) && this.documentsExist(this.getESInstance(this.esHost, this.esPort),indexEncription, type, credentialNonCommonFriendsIds) ) {
					
				relationDocumentscreated = true;
				
			}
			 
			
		
		} catch (Exception e) {
			
			log.error(e.getMessage());

		}
			
		return relationDocumentscreated;
	}

	 
	 /**
		 * use to delete elasticSearch mapping older than 3 days 
		 * 
		 * @param client
		 * @param indexname
		 * @return typeName
		 */

	public  boolean deleteOldTweetsByDate(TransportClient client,String index,String type,String deletDate) throws Exception {
		
		boolean deleted = false;
		try {
			
		
		// getting all types of a index then delete older than 3 days
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();			
		
		BoolFilterBuilder boolFilterBuilder = new BoolFilterBuilder();
		
		Calendar cal = Calendar.getInstance();
	    cal.add(Calendar.DATE, -7);
	    String SevenDaysOldDate = format.format(cal.getTime()).toString();
		System.err.println("3 days old "+deletDate+" 7 days old"+SevenDaysOldDate);
		// range for date	
			
			boolFilterBuilder.must(FilterBuilders.rangeFilter("date").gte(SevenDaysOldDate).lte(deletDate));
				
			DeleteByQueryResponse response1 = client.prepareDeleteByQuery().setIndices(index).setTypes("tweets")
				       .setQuery(QueryBuilders.filteredQuery(boolQuery,boolFilterBuilder))
				       .execute()
				       .actionGet();
		
		System.out.println("deleted !"+response1.status());
		if (response1.status().toString().equals("OK")) {
			deleted = true;
		}

		
			

		} catch (Exception e) {
				System.err.println(e);
			 }
				
				
				client.close();
				return deleted;
	}
	
	 /**
		 * use to delete elasticSearch mapping older than 3 days 
		 * 
		 * @param client
		 * @param indexname
		 * @return typeName
		 */

	public  boolean deleteOnlyMaping(TransportClient client,String index,String type) throws Exception {
		
		boolean deleted = false;
		try {
			
		
		// getting all types of a index then delete older than 3 days
		ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet(); 
		
		ImmutableOpenMap<String,MappingMetaData> mappings = resp.getState().metaData().index(index).mappings(); 
		if (mappings.containsKey(type)) { 
			DeleteMappingResponse response = client.admin().indices().prepareDeleteMapping(index).setType(type).execute().actionGet(); 
			System.out.println("deleted !"+response.isAcknowledged());
			deleted = response.isAcknowledged();
		} 
				} catch (Exception e) {
					log.error(e.getMessage());
//					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

						}
				
				
				client.close();
				return deleted;
	}
	 
	 /**
	 * use to delete elasticSearch mapping older than 3 days 
	 * 
	 * @param client
	 * @param indexname
	 * @return typeName
	 */

	public  String getIndexMaping(TransportClient client,String index) throws Exception {
	
		String types = null;
	try {
	// getting all types of a index then delete older than 3 days
	ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet(); 
	
	ImmutableOpenMap<String,MappingMetaData> mappings = resp.getState().metaData().index(index).mappings(); 
	 types = mappings.keys().toString();
			
		} catch (Exception e) {
			log.error(e.getMessage());
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
			client.close();
			return types;
}
	
	public ArrayList<Map<String, Object>> searchDocuments(TransportClient client,String indexName, String field,String keywords[],int page, int documentsSize) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		try {
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			for (String keyword : keywords) {
				boolQuery.should(QueryBuilders.matchPhraseQuery(field, keyword));
			}
			SearchResponse response = client.prepareSearch(indexName)
					.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery)
					.setFrom(page).setSize(documentsSize).setExplain(true).execute()
					.actionGet();
			SearchHit[] results = response.getHits().getHits();
			Map<String, Object> totalCount = new HashMap<String, Object>();
			totalCount.put("totalCount", response.getHits().getTotalHits());

			documents.add(totalCount);
			for (SearchHit hit : results) {
				
				Map<String, Object> result = hit.getSource(); // the retrieved document	
				documents.add(result);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		
		// close client
		client.close();
		return documents;
	}
	
	public boolean deleteIndex(TransportClient client,String indexName){

		boolean isexist = indexExist(client, indexName);
		boolean	deleted = false;
		DeleteIndexResponse response = null;
		try {

		if (isexist) {
			 DeleteIndexRequest request = new DeleteIndexRequest(indexName);
			  response = client.admin().indices().delete(request).actionGet();// dell only index
			 
			 // dell mapping e.g (types)
//			 DeleteMappingResponse response = client.admin().indices().prepareDeleteMapping("e72c504dc16c8fcd2fe8c74bb492affa").setType("2017-03-29").execute().actionGet();
			 System.out.println("deleted !"+response.isAcknowledged());
			 deleted = response.isAcknowledged();
		}
			} catch (Exception e) {
				log.error(e.getMessage());
				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

					}
		
			return deleted; 
	}
	
	
	public ArrayList<Map<String, Object>> searchTweetDocuments(TransportClient client,String indexName,String types[], String fields[], String exWords[],String keyword,int page, int documentsSize,String sortOn,SortOrder order) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		try {
	
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		for (String field : fields) {
			
			if (field == "" || keyword == "") {
				break;
		}
			boolQuery.should(QueryBuilders.matchQuery(field, keyword).operator(Operator.AND));

		}
		
		// query for exact match -ve search
		for (int f = 0; f < fields.length; f++) {
						
			for (int exPosition = 0; exPosition < exWords.length; exPosition++) {
				
			if (fields[f] == "" || exWords[exPosition] == "") {
					break;
			}
			
			boolQuery.mustNot(QueryBuilders.matchQuery(fields[f],exWords[exPosition]).operator(Operator.AND));
		}
	}
																				
		SearchResponse response = client.prepareSearch(indexName).setTypes(types[0])  
				.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery).addSort(sortOn,order)
				.setFrom(page).setSize(documentsSize).setExplain(true).execute()
				.actionGet();
		SearchHit[] results = response.getHits().getHits();
		Map<String, Object> totalCount = new HashMap<String, Object>();
		totalCount.put("totalCount", response.getHits().getTotalHits());

		documents.add(totalCount);
		for (SearchHit hit : results) {
			
			Map<String, Object> result = hit.getSource(); // the retrieved document
			/*if (result.get("externalUrl").toString()=="") {
				result.put("externalUrl", "N/A");
			}
			else {

					result.put("externalUrl", this.expandUrl(result.get("externalUrl").toString()));		
			}*/
			documents.add(result);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		// close client
		client.close();
		return documents;
	}
	

	public ArrayList<Map<String, Object>> searchUserRelationDocuments(TransportClient client,String indexName, String fields[],String keyword,int page, int documentsSize) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		try {
			
		
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (String field : fields) {
			boolQuery.should(QueryBuilders.matchPhraseQuery(field, keyword));
		}
		SearchResponse response = client.prepareSearch(indexName)
				.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery).addSort("date",SortOrder.DESC)
				.setFrom(page).setSize(documentsSize).setExplain(true).execute()
				                                                                                                                                                                                                                                                                                                                                                                .actionGet();
		SearchHit[] results = response.getHits().getHits();

		for (SearchHit hit : results) {
			
			Map<String, Object> result = hit.getSource(); // the retrieved document
			documents.add(result);
		}
			} catch (Exception e) {
				log.error(e.getMessage());
				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

		}
		// close client
		client.close();
		return documents;
	}
	
	// GENERAL FUNCTION TO get any type data SEARCH
			public ArrayList<Map<String, Object>> getTypeData(TransportClient client,String index, String type,int page,int size,String sortOn,SortOrder order) {
				ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
				try {
					
				SearchResponse response = client.prepareSearch(index).setTypes(type)
						.setSearchType(SearchType.QUERY_THEN_FETCH).addSort(sortOn,order).setFrom(page)
						.setSize(size)
						.execute().actionGet();
				Map<String, Object> totalCount = new HashMap<String, Object>();
				totalCount.put("totalCount", response.getHits().getTotalHits());
				documents.add(totalCount);
				for (SearchHit hit : response.getHits()) {
					Map<String, Object> result = hit.getSource(); // the retrieved document
					documents.add(result);
				}
				
					} catch (Exception e) {
						log.error(e.getMessage());
						log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

					}
				client.close();
				return documents;
			}

	// GENERAL FUNCTION TO SEARCH
		public ArrayList<Map<String, Object>> homePageData(TransportClient client,String index, String types[],int page,int size,String sortOn,SortOrder order) {
			ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		
			try {

			SearchResponse response = client.prepareSearch(index).setTypes(types[0])
					.setSearchType(SearchType.QUERY_THEN_FETCH).addSort(sortOn,order).setFrom(page)
					.setSize(size)
					.execute().actionGet();
			Map<String, Object> totalCount = new HashMap<String, Object>();
			totalCount.put("totalCount", response.getHits().getTotalHits());
			documents.add(totalCount);
			for (SearchHit hit : response.getHits()) {
				Map<String, Object> result = hit.getSource(); // the retrieved document
				/*if (result.get("externalUrl").toString()=="") {
					result.put("externalUrl", "N/A");
				}
				else {
					
					result.put("externalUrl", this.expandUrl(result.get("externalUrl").toString()));
				}*/
				documents.add(result);
			}
				} catch (Exception e) {
					log.error(e.getMessage());
					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				}
			client.close();
			return documents;
		}
		
		// Search using Range QUERY
		public ArrayList<Map<String, Object>>  rangeFilter(TransportClient client,String index,String type,String keyword,String searchIn[],String exWords[],String fields[], String from[],String to[],int page,int documentsSize,String sortOn,SortOrder order) {
			ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
			try {

			BoolQueryBuilder boolquery = new BoolQueryBuilder();			
			
			BoolFilterBuilder boolFilterBuilder = new BoolFilterBuilder();
			
			// query for exact match +ve search
			for (int j = 0; j < searchIn.length; j++) {
				if (searchIn[j] == "" || keyword == "") {
					break;
				}
				
				boolquery.should(QueryBuilders.matchQuery(searchIn[j], keyword).operator(Operator.AND));
				
			}
			
			// query for exact match -ve search
			for (int f = 0; f < searchIn.length; f++) {
				
				for (int exPosition = 0; exPosition < exWords.length; exPosition++) {
					if (searchIn[f] == "" || exWords[exPosition] == "") {
						break;
					}
					boolquery.mustNot(QueryBuilders.matchQuery(searchIn[f],exWords[exPosition]).operator(Operator.AND));
				}
			}
			
			
			
			for (int i = 0 ; i<fields.length ; i++) {
				
				if (from[i].equals("")) {
					from[i] = "0";
				}
				if (to[i].equals("") ) {
					to[i] = "50000000000";
				}
				
				boolFilterBuilder.must(FilterBuilders.rangeFilter(fields[i]).gte(from[i]).lte(to[i]));
			}
			

			SearchResponse response = client.prepareSearch(index).setTypes(type)
					.setSearchType(SearchType.QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.filteredQuery(boolquery,boolFilterBuilder))
					.addSort(sortOn,order)
					.setFrom(page).setSize(documentsSize).setExplain(true).execute()
					.actionGet();
			
			SearchHit[] results = response.getHits().getHits();
		
			Map<String, Object> totalSize = new HashMap<String, Object>();
			totalSize.put("totalCount", response.getHits().getTotalHits());
			documents.add(totalSize);
			for (SearchHit hit : results) {

				Map<String, Object> result = hit.getSource(); // the retrieved document
				documents.add(result);
			}
				} catch (Exception e) {
					log.error(e.getMessage());
					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				}
			
			// close client
			client.close();
			return documents;
			
			
			}
	
	
	public boolean documentsExist(TransportClient client, String index, String type,String[] ids) throws Exception {
		boolean success = false;
		try {
			
			if (ids.length > 0) {
				MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
					    .add(index, type, ids)           
					    .get();

					for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
					    GetResponse response = itemResponse.getResponse();
					    if (response.isExists()) {                      
					       success = true;
					    }
					}
			}
		else {
			success = true;
		}
	
				} catch (Exception e) {
					log.error(e.getMessage());
					log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

				}
			client.close();
			return success;
			
	}
	
		public ArrayList<Map<String, Object>> searchUserDocumentsByIds(TransportClient client, String index, String type,String[] ids) throws Exception {
			
			ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
			try {
				
		
			MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
				    .add(index,type, ids)           
				    .get();
			
				for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
				    GetResponse response = itemResponse.getResponse();
				    if (response.isExists()) {                      
				        Map<String, Object> map = response.getSourceAsMap(); 
				      
				        documents.add(map);
				    }
				}
			} catch (Exception e) {
				log.error(e.getMessage());
//				log.error("error in "+Thread.currentThread().getStackTrace()[2].getClassName()+Thread.currentThread().getStackTrace()[2].getMethodName()+Thread.currentThread().getStackTrace()[2].getLineNumber());

			}
			// close client
			client.close();
			return documents;
		}
		
	
	public  boolean indexExist(TransportClient client,String INDEX_NAME) {
		IndexMetaData indexMetaData = null;
		try {
			
		
		 indexMetaData = client.admin().cluster().state(Requests.clusterStateRequest())
	            .actionGet()
	            .getState()
	            .getMetaData()
	            .index(INDEX_NAME);
				} catch (Exception e) {
					
				}
	    return (indexMetaData != null);	
	   
	}


}
