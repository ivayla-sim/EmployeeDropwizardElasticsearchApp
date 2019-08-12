package employee.dropwizard.elasticsearch.pck.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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
					"		\"_doc\" : {\n" +
					"			\"properties\" : {\n" +
					"				\"id\" : {\"type\" : \"text\"},\n" +
					"				\"isActive\" : {\"type\" : \"boolean\"},\n" +
					"				\"name\" : {\"type\" : \"nested\",\n" +
					"					\"properties\" : {\n" +
					"						\"firstName\" : {\"type\" : \"text\"},\n" +
					"						\"lastName\" : {\"type\" : \"text\"}\n" +
					"					}\n" +
					"				},\n" +
					"				\"designation\" : {\"type\" : \"text\"},\n" +
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
					"		}\n" +
					"	}\n" +
					"}";
						
			//XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(sourceString);
			
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX).source(/*xContentBuilder*/sourceString, XContentType.JSON);
			
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
		
		List<SearchHit> searchHits = Arrays.asList(searchResponse.getHits().getHits());
		List<Employee> results = new ArrayList<Employee>();
		searchHits.forEach(hit -> results.add(objectMapper.convertValue(hit.getSourceAsMap(), Employee.class)));
		
		return results;
		
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
		}
		
		System.out.println("Total uploaded " + count);
		//client.close();
		br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

}
