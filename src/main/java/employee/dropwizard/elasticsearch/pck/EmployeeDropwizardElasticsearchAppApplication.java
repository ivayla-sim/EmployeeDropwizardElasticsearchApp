package employee.dropwizard.elasticsearch.pck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import employee.dropwizard.elasticsearch.pck.resources.EmployeeRestResources;
import employee.dropwizard.elasticsearch.pck.service.EmployeeRestService;
import employee.dropwizard.elasticsearch.pck.client.ElasticsearchRestClient;

public class EmployeeDropwizardElasticsearchAppApplication extends Application<EmployeeDropwizardElasticsearchAppConfiguration> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeDropwizardElasticsearchAppApplication.class);
	

    public static void main(final String[] args) throws Exception {
        new EmployeeDropwizardElasticsearchAppApplication().run(args);
        
    }

    @Override
    public String getName() {
        return "EmployeeDropwizardElasticsearchApp";
    }

    @Override
    public void initialize(final Bootstrap<EmployeeDropwizardElasticsearchAppConfiguration> bootstrap) {
        // TODO: application initialization
    	
    	
    	
    }

    @Override
    public void run(final EmployeeDropwizardElasticsearchAppConfiguration configuration,
                    final Environment environment) throws Exception {
    	
    	//employeeRestService.createMapping();
    	
    	final ElasticsearchRestClient elasticRestClient = new ElasticsearchRestClient(configuration);
    	
    	EmployeeRestService employeeRestService= new EmployeeRestService(elasticRestClient.getRestHighLevelClient(), configuration.objectMapper());
    	
    	
    	environment.lifecycle().manage(elasticRestClient);
    	
    	LOGGER.info("Registering REST resource");
    	
    	//employeeRestService.bulkUploadEmployeeDoc();
    	
    	employeeRestService.createIndexMapping();
    	
    	environment.jersey().register(new EmployeeRestResources(employeeRestService, environment.getValidator(), elasticRestClient));
    	
    	
    }

}
