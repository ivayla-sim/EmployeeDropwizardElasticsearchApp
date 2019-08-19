package employee.dropwizard.elasticsearch.pck.resources;

import java.io.IOException;
//import java.util.Map;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import employee.dropwizard.elasticsearch.pck.api.AggregationBuckets;
import employee.dropwizard.elasticsearch.pck.client.ElasticsearchRestClient;

//import org.elasticsearch.action.get.GetRequest;
//import org.elasticsearch.action.get.GetResponse;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestHighLevelClient;

import employee.dropwizard.elasticsearch.pck.core.Employee;
import employee.dropwizard.elasticsearch.pck.service.EmployeeRestService;



@Path("/employees")
@Produces(MediaType.APPLICATION_JSON)
public class EmployeeRestResources {
	
	private static final String INDEX = "employees";
	
	
	private EmployeeRestService employeeRestService;
	
	private final Validator validator;
	
	public EmployeeRestResources(EmployeeRestService employeeRestService, Validator validator, ElasticsearchRestClient elasticRestClient) {
		this.employeeRestService = employeeRestService;
		this.validator = validator;
	}
	
	
	@POST
	@Path("/_create")
	public Response createEmployee(Employee employee) throws IOException, URISyntaxException {
		
		//validation start
		Set<ConstraintViolation<Employee>> violations = validator.validate(employee); 
		if(violations.size() > 0) {
			ArrayList<String> validationMessages = new ArrayList<String>();
			for(ConstraintViolation<Employee> violation : violations) {
				validationMessages.add(violation.getPropertyPath().toString() + ": " + violation.getMessage());
			}
			return Response.status(Status.BAD_REQUEST).entity(validationMessages).build();			
		}
		//validation end
		
		if(employee != null) {
			employeeRestService.createEmployeeDoc(employee);
			return Response.created(new URI("/employees/" + employee.getId()))
	                .build();
		} else {
			return Response.status(Status.NOT_FOUND).build();
		}
	
	}
	
	@GET
	@Path("/_doc/{id}")
	public Response getEmployeeById(@PathParam("id") String id) throws IOException {
		
		System.out.println ("Start Resource getEmployeeById" + id);

		Employee fetchedEmployee = employeeRestService.findById(id);
		
		System.out.println ("Start fetchedEmployee" + fetchedEmployee);
		
		if (fetchedEmployee != null)
			return Response.ok(fetchedEmployee).build();
		else
			return Response.status(Status.NOT_FOUND).build();	
	}
	
	
	@GET
	@Path("/_all")
	public Response getEmployeesAll() throws IOException {
		
		List<Employee> fetchedEmployeeList = employeeRestService.findAll();
		
		if (!fetchedEmployeeList.isEmpty()) {
			return Response.ok(fetchedEmployeeList).build();
		} else {
			return Response.status(Status.NO_CONTENT).build();
		}
	}
	
	
	@PUT
	@Path("/_update/{id}")
	public Response updateEmployeeById(@PathParam("id") String id, Employee employee) throws IOException {
		
		//validation start
		Set<ConstraintViolation<Employee>> violations = validator.validate(employee);
		if(violations.size() > 0) {
			ArrayList<String> validationMessages = new ArrayList<String>();
			for(ConstraintViolation<Employee> violation : violations) {
				validationMessages.add(violation.getPropertyPath().toString() + ": " + violation.getMessage());
			}
			return Response.status(Status.BAD_REQUEST).entity(validationMessages).build();
		}
		//validation end
		
		if(employee != null) {
			employeeRestService.updateEmployeeDoc(employee);
			return Response.accepted(employee).build();
		} else {
			return Response.notModified().build();
		}	
		
	}
	
	
	@PUT
	@Path("/_bulk")
	public Response bulkUploadEmployeeDoc() throws IOException {
		
			employeeRestService.bulkUploadEmployeeDoc();
			return Response.ok().build();
		
	}
	
	
	@DELETE
	@Path("/_delete/{id}")
	public Response deleteEmployeeById (@PathParam("id") String id) throws IOException {
		
		Employee employeeForDetete = employeeRestService.findById(id);
		
		System.out.println(employeeForDetete);
		
		if(employeeForDetete != null) {
			employeeRestService.deleteEmployeeDoc(id);
			return Response.ok("Document " + id + " is successfully deleted").build();
			
		} else {
			return Response.ok("Document " + id + " does not exist").status(Status.NOT_FOUND).build();
					
		}	
	}
	
	
	@DELETE
	@Path("/delete_index")
	public Response deleteIndex() throws IOException {
		
		Boolean isIndexDeleted = employeeRestService.deleteIndex(INDEX);
		
		Response deleteIndexResult = (isIndexDeleted) ? 
					Response.ok("Index " + INDEX + " is successfully deleted").build() : 
						Response.ok("Index " + INDEX + " does not exist").status(Status.NOT_FOUND).build();
					
		return deleteIndexResult;
		
		
	}
	
	
	
	@GET					//e.g. call 	http://localhost:8080/employees/search?city=Gradets
	@Path("/_search")
	public Response searchByAddressAttribute(
			@QueryParam(value = "city") String city,
			@QueryParam(value = "country") String country,
			@QueryParam(value = "state") String state,
			@QueryParam(value = "streetNameAndNumber") String streetNameAndNumber,
			@QueryParam(value = "zipCode") Integer zipCode
												 ) throws IOException {
		
		List<Employee> employeeListByAddress = employeeRestService.searchByAddressAttribute(city, country, state, streetNameAndNumber, zipCode);
		
		return Response.ok(employeeListByAddress).build();
		
	}
	
	@GET	//e.g. call 	http://localhost:8080/employees/_search/dateOfJoining?startDate=2016-04-02&endDate=2017-04-02
	@Path("/_search/dateOfJoining")
	public Response searchByDateOfJoining (
			@QueryParam(value = "startDate") String startDateStr,
			@QueryParam(value = "endDate") String endDateStr
			) throws IOException, ParseException {
		
		System.out.println("REST end point dateOfJoining");
		
		List<Employee> employeeListByDateOfJoining = employeeRestService.searchByRangeDateOfJoining(startDateStr, endDateStr);
		
		return Response.ok(employeeListByDateOfJoining).build();
		
	}
	
	
	@GET	//e.g. call 	http://localhost:8080/employees/_search/dateOfJoining?startDate=2016-04-02&endDate=2017-04-02
	@Path("/_search/dateOfJoining/designations/agg")
	public Response searchByRangeDateOfJoiningAggByDesignations (
			@QueryParam(value = "startDate") String startDateStr,
			@QueryParam(value = "endDate") String endDateStr
			) throws IOException, ParseException {
		
		System.out.println("REST end point dateOfJoiningV2");
		
		//List<Employee> employeeListByDateOfJoining = employeeRestService.searchByRangeDateOfJoiningV2(startDateStr, endDateStr);
		
		/*Map<String, Long>*/ List<AggregationBuckets> results = employeeRestService.searchByRangeDateOfJoiningAggByDesignations(startDateStr, endDateStr);
		
		return Response.ok(results).build();
		
	}
	
	
	@GET
	@Path("/_search/nearest")
	public Response searchGeoNearestEmployees (
			@QueryParam(value = "latitude") double latCenterPoint,
			@QueryParam(value = "longitude") double lonCenterPoint,
			@QueryParam(value = "distance") double distEntered
			) throws IOException {
		
		System.out.println("Start REST end point GeoLocation");
		
		List<Employee> employeeListNearest = employeeRestService.searchGeoNearestEmployees(latCenterPoint, lonCenterPoint, distEntered);
		
		System.out.println("End REST end point GeoLocation");
		
		return Response.ok(employeeListNearest).build();
		
	}
	

}



