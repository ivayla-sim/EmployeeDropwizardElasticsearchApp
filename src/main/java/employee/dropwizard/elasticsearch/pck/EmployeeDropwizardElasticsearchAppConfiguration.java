package employee.dropwizard.elasticsearch.pck;

import io.dropwizard.Configuration;
//import io.dropwizard.validation.ValidationMethod;
import io.dropwizard.validation.ValidationMethod;

import com.fasterxml.jackson.annotation.JsonIgnore;
//import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
//import org.hibernate.validator.constraints.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;

import java.util.Collections;
import java.util.List;
import java.util.Map;

//import java.util.Collections;
//import java.util.Map;

import javax.validation.constraints.*;

public class EmployeeDropwizardElasticsearchAppConfiguration extends Configuration {
    
    @JsonProperty
    @NotNull
    private List<String> servers = Collections.emptyList();

    @JsonProperty
    @NotEmpty
    private String clusterName = "elasticsearch";

    @JsonProperty
    @NotNull
    private Map<String, String> settings = Collections.emptyMap();

    @JsonProperty
    private Map<String, String> headers = Collections.emptyMap();

    @JsonProperty
    private String settingsFile = null;

    @JsonProperty
    private boolean transportClient = false;

    //@JsonProperty
    //private EsSnifferConfiguration sniffer = new EsSnifferConfiguration();

    public List<String> getServers() {
        return servers;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    public String getSettingsFile() {
        return settingsFile;
    }

    public boolean isTransportClient() {
        return transportClient;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    /*
    public EsSnifferConfiguration getSniffer() {
        return sniffer;
    }
    */

    @ValidationMethod
    @JsonIgnore
    public boolean isValidConfig() {
        return !servers.isEmpty();
    }
	
	
	public ObjectMapper objectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
		
		return objectMapper;
		
	}
	
}
