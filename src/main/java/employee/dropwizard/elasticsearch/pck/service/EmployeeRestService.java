package employee.dropwizard.elasticsearch.pck.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.lucene.index.Terms;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
//import org.elasticsearch.action.admin.indices.create.CreateIndexRequest; //deprecated
import org.elasticsearch.client.indices.CreateIndexRequest;
//import org.elasticsearch.action.admin.indices.create.CreateIndexResponse; //deprecated
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
//import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import employee.dropwizard.elasticsearch.pck.api.AggregationBuckets;
import employee.dropwizard.elasticsearch.pck.core.Employee;


public class EmployeeRestService {
	
	
	private RestHighLevelClient client = employee.dropwizard.elasticsearch.pck.client.ElasticsearchRestClient.getRestHighLevelClient();
	
	private ObjectMapper objectMapper;
	
	//public EmployeeRestService() {};
	
	public EmployeeRestService(RestHighLevelClient client, ObjectMapper objectMapper) {
		this.client = client;
		this.objectMapper = objectMapper;
	}
	
	private static final String INDEX = "employees";
	
	
	public boolean createIndexMapping() throws IOException {
		
		GetIndexRequest request = new GetIndexRequest(INDEX);
		
		boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
		
		System.out.println(exists);
		
		if(!exists) {
			
			String sourceString = 
					"{\n" +
					"	\"settings\" : {\n" +
					"		\"number_of_shards\" : 1,\n" +
					"		\"number_of_replicas\" : 0\n" +
					"	},\n" +
					"	\"mappings\" : {\n" +
					//"		\"_doc\" : {\n" +
					"			\"properties\" : {\n" +
					"				\"id\" : {\"type\" : \"text\"},\n" +
					"				\"isActive\" : {\"type\" : \"boolean\"},\n" +
					"				\"name\" : {\"type\" : \"nested\",\n" +
					"					\"properties\" : {\n" +
					"						\"firstName\" : {\"type\" : \"text\"},\n" +
					"						\"lastName\" : {\"type\" : \"text\"}\n" +
					"					}\n" +
					"				},\n" +
					//"				\"designation\" : {\"type\" : \"keyword\"},\n" +
					"				\"designation\" : {\n" +
					"					\"type\" : \"text\",\n" +
					"					\"fields\" : {\n" +
					"						\"keyword\" : {\"type\" : \"keyword\"}\n" +
					"					}\n" +
					"				},\n" +	
					"				\"dateOfJoining\" : {\"type\" : \"date\"},\n" +
					"				\"salary\" : {\"type\" : \"text\"},\n" +
					"				\"picture\" : {\"type\" : \"text\"},\n" +
					"				\"age\" : {\"type\" : \"integer\"},\n" +
					"				\"gender\" : {\"type\" : \"text\"},\n" +
					"				\"eyeColor\" : {\"type\" : \"text\"},\n" +
					"				\"company\" : {\"type\" : \"text\"},\n" +
					"				\"email\" : {\"type\" : \"text\"},\n" +
					"				\"phone\" : {\"type\" : \"text\"},\n" +
					"				\"addresses\" : {\"type\" : \"nested\",\n" +
					"					\"properties\" : {\n" +
					"						\"id\" : {\"type\" : \"integer\"},\n" +
					"						\"country\" : {\"type\" : \"text\"},\n" +
					"						\"state\" : {\"type\" : \"text\"},\n" +
					"						\"city\" : {\"type\" : \"text\"},\n" +
					"						\"streetNameAndNumber\" : {\"type\" : \"text\"},\n" +
					"						\"zipCode\" : {\"type\" : \"integer\"}\n" +
					"					}\n" +
					"				},\n" +
					"				\"location\" : {\"type\" : \"geo_point\"},\n" +
					"				\"about\" : {\"type\" : \"text\"},\n" +
					"				\"interests\" : {\"type\" : \"text\"},\n" +
					"				\"friends\" : {\"type\" : \"nested\",\n" +
					"					\"properties\" : {\n" +
					"						\"id\" : {\"type\" : \"integer\"},\n" +
					"						\"name\" : {\"type\" : \"text\"}\n" +
					"					}\n" +
					"				},\n" +
					"				\"greeting\" : {\"type\" : \"text\"}\n" +
					"			}\n" +
					//"		}\n" +
					"	}\n" +
					"}";
						
			//XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(sourceString);
			
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX).source(sourceString, XContentType.JSON); /*xContentBuilder*/
			
			CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
			
			System.out.println("Created index: " + INDEX + " : " + createIndexResponse.isAcknowledged());
			
			return createIndexResponse.isAcknowledged();
			
			
		} else {
			
			System.out.println ("Index already exists");
			
			return exists;
			
		}
		
	}
	
	
	public String createEmployeeDoc(Employee employee) throws IOException {
		System.out.println ("Start Service layer POST");
		
		//Map<String, Object> documentMapper = objectMapper.convertValue(employee, Map.class);
		
		Map<String, Object> documentMapper = objectMapper.convertValue(employee, new TypeReference<Map<String, Object>>() {});
		
		//the constructor is deprecated
		//IndexRequest indexRequest = new IndexRequest(INDEX, TYPE_POST, employee.getId()).source(documentMapper);
		
		IndexRequest indexRequest = new IndexRequest(INDEX)
				.id(employee.getId())
				.source(documentMapper);
		
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		
		System.out.println ("End Service layer POST");
		
		return indexResponse
				.getResult()
				.name();
		
	}
	
	
	
	public Employee  findById (String id) throws IOException {
		System.out.println ("Start Service layer GET" + id);
		
		GetRequest getRequest = new GetRequest(INDEX).id(id);
		
		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
		
		Map<String, Object> resultMap = getResponse.getSource();
		
		System.out.println ("End Service layer GET");
			
		return objectMapper.convertValue(resultMap, Employee.class);
		
	
	}
	
	
	public List<Employee> findAll() throws IOException {
		
		SearchRequest searchRequest = new SearchRequest(INDEX);
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		return convertSearchHitsToListEmployees(searchResponse);
		
	}
	
	
	public String updateEmployeeDoc (Employee employee) throws IOException {
		
		System.out.println("Start Service layer PUT");
		
		//Employee resultEmployee = findById(employee.getId());
		
		UpdateRequest updateRequest = new UpdateRequest().index(INDEX).id(employee.getId());
		
		//Map<String, Object> documentMapper = objectMapper.convertValue(employee, Map.class);
		
		Map<String, Object> documentMapper = objectMapper.convertValue(employee, new TypeReference<Map<String, Object>>() {});
		
		
		updateRequest.doc(documentMapper);
		
		UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
		
		System.out.println("End Service layer PUT");
		
		return updateResponse.getResult().name();	
		
	}
	
	
	public String deleteEmployeeDoc (String id) throws IOException {
		
		System.out.println("Start Service layer DELETE");
		
		DeleteRequest deleteRequest = new DeleteRequest(INDEX, id);
		DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
		
		System.out.println("End Service layer DELETE");
		
		return deleteResponse.getResult().name();
	
	}
	
	
	public Boolean deleteIndex (String index) throws IOException {
		
		System.out.println("Start Service layer DELETE INDEX");
		
		org.elasticsearch.action.support.master.AcknowledgedResponse deleteIndexResponse = new org.elasticsearch.action.support.master.AcknowledgedResponse(false);
		
		try {				
		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);		
		deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);	
		//return deleteIndexResponse.isAcknowledged();
		}
		catch (ElasticsearchException esException) {
			if (esException.status() == RestStatus.NOT_FOUND) {
				System.out.println("Index " + index + "does not exist.");
			}
		}
		
		System.out.println("End Service layer DELETE INDEX");
		return deleteIndexResponse.isAcknowledged();
	
	}

	
	
	
	public void bulkUploadEmployeeDoc() throws IOException {
		
		try {
		
		BulkRequest bulkRequest = new BulkRequest();
		
		int count = 0;
		int batch = 10;
		
		BufferedReader br = new BufferedReader(new FileReader("Employees100GeneratedV2ND.json"));
		
		String line;
		
		while((line = br.readLine()) != null) {
			
			String idFromLine = line.substring(line.indexOf("id") + 4, line.indexOf(','));
			
			Employee employee = objectMapper.readValue(line, Employee.class);
			
			Map<String, Object> documentMapper = objectMapper.convertValue(employee, new TypeReference<Map<String, Object>>() {});

			bulkRequest.add(new IndexRequest(INDEX).id(idFromLine).source(documentMapper/*line, XContentType.JSON*/));
			count++;
			
			if(count%batch == 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				if(bulkResponse.hasFailures()) {
					for(BulkItemResponse bulkItemResponse: bulkResponse) {
						if(bulkItemResponse.isFailed()) {
							BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
							System.out.println("Error " + failure.toString());
							
						}
					}
				}
				System.out.println("Uploaded " + count + " so far");
				bulkRequest = new BulkRequest();
				//bulkResponse.getItems().toString();
			}
		}
		
		if(bulkRequest.numberOfActions() > 0) {
			BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
			if(bulkResponse.hasFailures()) {
				for (BulkItemResponse bulkItemResponse : bulkResponse) {
					if (bulkItemResponse.isFailed()) {
						BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
						System.out.println("Error " + failure.toString());
					}
				}
			}
			//bulkResponse.getItems().toString();
		}
		
		System.out.println("Total uploaded " + count);
		//client.close();
		br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	
	public List<Employee> searchByAddressAttribute(String city, 
													String country, 
													String state, 
													String streetNameAndNumber, 
													Integer zipCode) throws IOException {
		
		System.out.println("Start Service layer SEARCH CITY");
		
		String queryAddressValue = (city != null) ? city :
			(country != null ? country : 
				(state != null ? state :
					(streetNameAndNumber != null ? streetNameAndNumber :
						(zipCode.toString() != null ? zipCode.toString() : " " ))));
		
		System.out.println(queryAddressValue);

		String queryAddressAttr = (city != null) ? "city" :
			(country != null ? "country" : 
				(state != null ? "state" :
					(streetNameAndNumber != null ? "streetNameAndNumber" :
						(zipCode.toString() != null ? "zipCode" : " " ))));
		
		System.out.println(queryAddressAttr);

		SearchRequest searchRequest = new SearchRequest();

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		QueryBuilder queryBuilder = QueryBuilders
				.boolQuery()
				.must(QueryBuilders.matchQuery("addresses." + queryAddressAttr, queryAddressValue));


		searchSourceBuilder.query(QueryBuilders
				.nestedQuery("addresses", queryBuilder, ScoreMode.Avg));


		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

		return convertSearchHitsToListEmployees(searchResponse);


	}
	
	
	public List<Employee> searchByRangeDateOfJoining(String startDateStr, String endDateStr) throws IOException, ParseException {
		
		System.out.println("Start Service layer SEARCH Date");
		
		startDateStr = (startDateStr == null || startDateStr.isEmpty()) ? "1900-01-01" : startDateStr;
		endDateStr = (endDateStr == null || endDateStr.isEmpty()) ? "2500-12-31" : endDateStr;
		
		Date startDate = getDateFromString(startDateStr);
		Date endDate = getDateFromString(endDateStr);
		
		SearchRequest searchRequest = new SearchRequest();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		
		QueryBuilder queryBuilder = QueryBuilders.rangeQuery("dateOfJoining").gte(startDate).lte(endDate).boost(2);
		
		searchSourceBuilder.query(queryBuilder);
		
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		System.out.println("End Service layer SEARCH Date");
		
		return convertSearchHitsToListEmployees(searchResponse);
		
		
	}
	
	
	public /*Map<String, Long>*/ List<AggregationBuckets> searchByRangeDateOfJoiningAggByDesignations(String startDateStr, String endDateStr) throws IOException, ParseException {
		
		System.out.println("Start Service layer SEARCH Date V2");
		
		startDateStr = (startDateStr == null || startDateStr.isEmpty()) ? "1900-01-01" : startDateStr;
		endDateStr = (endDateStr == null || endDateStr.isEmpty()) ? "2500-12-31" : endDateStr;
		
		Date startDate = getDateFromString(startDateStr);
		Date endDate = getDateFromString(endDateStr);
		
		SearchRequest searchRequest = new SearchRequest();		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		
		QueryBuilder queryBuilder = QueryBuilders.rangeQuery("dateOfJoining").gte(startDate).lte(endDate).boost(2);
		
		TermsAggregationBuilder bucketByDesignation = AggregationBuilders.terms("designations").field("designation.keyword")
														.subAggregation(AggregationBuilders.min("minDateOfJoining").field("dateOfJoining"))
														.subAggregation(AggregationBuilders.max("maxDateOfJoining").field("dateOfJoining"))
														.subAggregation(AggregationBuilders.avg("avgAge").field("age"));
					
		searchSourceBuilder.query(queryBuilder).size(0).aggregation(bucketByDesignation); //without size:0 ES returns all hits
		
		searchRequest.source(searchSourceBuilder);
		
		System.out.println(searchRequest.source(searchSourceBuilder).toString());
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

		
		List<AggregationBuckets> aggregationBucketsList = new ArrayList<AggregationBuckets>();
		
		Terms designationsAgg = searchResponse.getAggregations().get("designations");
		
		for(Terms.Bucket designationsBucket : designationsAgg.getBuckets()) {
			
			AggregationBuckets aggregationBuckets = new AggregationBuckets();
			
			aggregationBuckets.setKey(designationsBucket.getKeyAsString());
			aggregationBuckets.setDocCount(designationsBucket.getDocCount());;
			
			Min minDateOfJoiningAgg = designationsBucket.getAggregations().get("minDateOfJoining");
			aggregationBuckets.setMinDateOfJoining(getDateFromString(minDateOfJoiningAgg.getValueAsString()));
			
			Max maxDateOfJoining = designationsBucket.getAggregations().get("maxDateOfJoining");	
			aggregationBuckets.setMaxDateOfJoining(getDateFromString(maxDateOfJoining.getValueAsString()));
			
			Avg avgAgeAgg = designationsBucket.getAggregations().get("avgAge");			
			aggregationBuckets.setAvgAge(avgAgeAgg.getValue());
			
			aggregationBucketsList.add(aggregationBuckets);
			
		}
	
		
	/*
		Map<String, Long> mapResult = new HashMap<String, Long>();
		
		Terms agg = searchResponse.getAggregations().get("designations");
		
		for(Bucket bucketEntry : agg.getBuckets()) {
			
			mapResult.put(bucketEntry.getKeyAsString(), bucketEntry.getDocCount());
			
			String key = bucketEntry.getKeyAsString();
			long docCount = bucketEntry.getDocCount();
			System.out.println("key = " + key + " docCount = " + docCount);
			
		}
		*/
		
		
		return aggregationBucketsList;
		
	}
	
	
	public List<Employee> searchGeoNearestEmployees(double latCenterPoint, double lonCenterPoint, double distEntered) throws IOException {
		
		System.out.println("Start Service layer GeoNearest");
		
		SearchRequest searchRequest = new SearchRequest();		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		
		QueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
		
		QueryBuilder geoDistanceQuery = QueryBuilders.geoDistanceQuery("location")
														.geoDistance(GeoDistance.ARC)
														.point(latCenterPoint, lonCenterPoint) //supplies lat, lon as an array and reverses the values!!!
														.distance(distEntered, DistanceUnit.KILOMETERS);
		
		QueryBuilder queryBuilder = QueryBuilders
				.boolQuery()
				.must(matchAllQuery)
				.filter(geoDistanceQuery);
		
		//searchSourceBuilder.fetchSource(false);
		String[] includeFields = new String[] {"name.firstName", "name.lastName", "phone", "email", "location", "sort"};
		String[] excludeFields = new String[] {};
		searchSourceBuilder.fetchSource(includeFields, excludeFields);
		
		GeoDistanceSortBuilder geoDistanceSorter = SortBuilders.geoDistanceSort("location", latCenterPoint, lonCenterPoint);
		geoDistanceSorter.order(SortOrder.ASC);
	
		
		searchSourceBuilder.query(queryBuilder).sort(geoDistanceSorter);
		
		//searchSourceBuilder.sort(geoDistanceSorter);
		
		
		
		searchRequest.source(searchSourceBuilder);
		
		
		
		System.out.println(searchRequest.toString());
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		//System.out.println(searchResponse.toString());
		
		System.out.println("End Service layer GeoNearest");
		
		return convertSearchHitsToListEmployees(searchResponse);
	}
	
	
	
	
	////////////////////////////////// HELPER METHODS ////////////////////////////////////////////////
	
	private List<Employee> convertSearchHitsToListEmployees(SearchResponse searchResponse) {
		
		List<SearchHit> searchHits = Arrays.asList(searchResponse.getHits().getHits());
		
		List<Employee> results = new ArrayList<Employee>();
		
		//searchHits.forEach(hit -> results.add(objectMapper.convertValue(hit.getSourceAsMap(), Employee.class)));
	
		searchHits.forEach(hit -> {			
			
			Map<String, Object> mapTemp = new HashMap<String, Object>();
			
			mapTemp = hit.getSourceAsMap();
			
			if(hit.getSortValues().length > 0) {
				mapTemp.put("distanceAway", 
									(double)(hit.getSortValues()[0].equals(null) ? 0 : hit.getSortValues()[0])
										/1000);
			}
			
			results.add(objectMapper.convertValue(mapTemp, Employee.class));		
									}
							);
		
		return results;
		
	}
	
	
	private Date getDateFromString(String dateString) throws ParseException {
		
		//dateString = dateString.length() > 10 ? dateString.substring(0, 10) : dateString;
		
	        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	        
	        Date date = df.parse(dateString);
	         
	        return date;
	   
	}

}
