package employee.dropwizard.elasticsearch.pck.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
//import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import employee.dropwizard.elasticsearch.pck.core.Employee;
import employee.dropwizard.elasticsearch.pck.EmployeeDropwizardElasticsearchAppConfiguration;
import employee.dropwizard.elasticsearch.pck.client.ElasticsearchRestClient;

public class EmployeeRestService {
	
	
	private RestHighLevelClient client = employee.dropwizard.elasticsearch.pck.client.ElasticsearchRestClient.getRestHighLevelClient();
	
	private ObjectMapper objectMapper;
	
	//public EmployeeRestService() {};
	
	public EmployeeRestService(RestHighLevelClient client, ObjectMapper objectMapper) {
		this.client = client;
		this.objectMapper = objectMapper;
	}
	
	private static final String INDEX = "employees";
	
	
	/*
	public Boolean createMapping () throws IOException {
		
		System.out.println("Start CREATE MAPPING");
		
		String sourceString = 
				"{\n" +
				"	\"settings\" : {\n" +
				"		\"number_of_shards\" : 1,\n" +
				"		\"number_of_replicas\" : 0\n" +
				"	},\n" +
				"	\"mappings\" : {\n" +
				"		\"employees\" : {\n" +
				"			\"properties\" : {\n" +
				"				\"id\" : {\"type\" : \"text\"},\n" +
				"				\"isActive\" : {\"type\" : \"boolean\"},\n" +
				"				\"name\" : {\"type\" : \"nested\",\n" +
				"					\"properties\" : {\n" +
				"						\"firstName\" : {\"type\" : \"text\"},\n" +
				"						\"lastName\" : {\"type\" : \"text\"}\n" +
				"					}\n" +
				"				}\n" +
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
				"				}\n" +
				"				\"latitude\" : {\"type\" : \"geo-point\"},\n" +
				"				\"longitude\" : {\"type\" : \"geo-point\"},\n" +
				"				\"about\" : {\"type\" : \"text\"},\n" +
				"				\"interests\" : {\"type\" : \"text\"},\n" +
				"				\"friends\" : {\"type\" : \"nested\",\n" +
				"					\"properties\" : {\n" +
				"						\"id\" : {\"type\" : \"integer\"},\n" +
				"						\"name\" : {\"type\" : \"text\"}\n" +
				"					}\n" +
				"				}\n" +
				"				\"greeting\" : {\"type\" : \"text\"},\n" +
				"			}\n" +
				"		}\n" +
				"	}\n" +
				"}";
		
		XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(sourceString);
		
		CreateIndexRequest mappingRequest = new CreateIndexRequest(INDEX).source(xContentBuilder);
		
		CreateIndexResponse createIndexResponse = client.indices().create(mappingRequest, RequestOptions.DEFAULT);
		
		System.out.println("End CREATE MAPPING");
		
		return createIndexResponse.isAcknowledged();
	
	}
	*/
	
	
	
	
	public String createEmployeeDoc(Employee employee) throws IOException {
		System.out.println ("Start Service layer POST");
		//UUID uuid = UUID.randomUUID();
	
		//employee.setEsId(Integer.toString(employeeCDTO.getId()));
		
		Map<String, Object> documentMapper = objectMapper.convertValue(employee, Map.class);
		
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
	
	
	public List<Employee> /*SearchHits*/ findAll() throws IOException {
		
		SearchRequest searchRequest = new SearchRequest(INDEX);
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		//return searchResponse.getHits();
		
		List<SearchHit> searchHits = Arrays.asList(searchResponse.getHits().getHits());
		List<Employee> results = new ArrayList<Employee>();
		searchHits.forEach(hit -> results.add(objectMapper.convertValue(hit.getSourceAsMap(), Employee.class)));
		
		return results;
		
	}
	
	
	public String updateEmployeeDoc (Employee employee) throws IOException {
		
		System.out.println("Start Service layer PUT");
		
		//Employee resultEmployee = findById(employee.getId());
		
		UpdateRequest updateRequest = new UpdateRequest().index(INDEX).id(employee.getId());
		
		Map<String, Object> documentMapper = objectMapper.convertValue(employee, Map.class);
		
		updateRequest.doc(documentMapper);
		
		UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
		
		System.out.println("End Service layer PUT");
		
		return updateResponse.getResult().name();	
		
	}
	
	public void bulkUploadEmployeeDoc() throws IOException {
		
		//Response response = client.getLowLevelClient().performRequest("HEAD", "/" + INDEX);
		
		try {
			
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
					"		\"employees\" : {\n" +
					"			\"properties\" : {\n" +
					"				\"id\" : {\"type\" : \"text\"},\n" +
					"				\"isActive\" : {\"type\" : \"boolean\"},\n" +
					"				\"name\" : {\"type\" : \"nested\",\n" +
					"					\"properties\" : {\n" +
					"						\"firstName\" : {\"type\" : \"text\"},\n" +
					"						\"lastName\" : {\"type\" : \"text\"}\n" +
					"					}\n" +
					"				}\n" +
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
					"				}\n" +
					"				\"latitude\" : {\"type\" : \"geo-point\"},\n" +
					"				\"longitude\" : {\"type\" : \"geo-point\"},\n" +
					"				\"about\" : {\"type\" : \"text\"},\n" +
					"				\"interests\" : {\"type\" : \"text\"},\n" +
					"				\"friends\" : {\"type\" : \"nested\",\n" +
					"					\"properties\" : {\n" +
					"						\"id\" : {\"type\" : \"integer\"},\n" +
					"						\"name\" : {\"type\" : \"text\"}\n" +
					"					}\n" +
					"				}\n" +
					"				\"greeting\" : {\"type\" : \"text\"},\n" +
					"			}\n" +
					"		}\n" +
					"	}\n" +
					"}";
			
			XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(sourceString);
			
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX).source(xContentBuilder);
			
			CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
			
			System.out.println("Created index: " + INDEX + " : " + createIndexResponse.isAcknowledged());
			
		} else {
			System.out.println ("Index already exists");
		}
		
		BulkRequest bulkRequest = new BulkRequest();
		
		int count = 0;
		int batch = 10;
		
		BufferedReader br = new BufferedReader(new FileReader("Employees100Generated.json"));
		
		String line;
		
		while((line = br.readLine()) != null) {
			
			bulkRequest.add(new IndexRequest(INDEX).source(line, XContentType.JSON));
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
		client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

}
