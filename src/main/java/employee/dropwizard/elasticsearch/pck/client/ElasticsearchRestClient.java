package employee.dropwizard.elasticsearch.pck.client;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import employee.dropwizard.elasticsearch.pck.EmployeeDropwizardElasticsearchAppConfiguration;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.util.Resources;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

//import com.google.common.io.Resources;
import org.elasticsearch.node.Node;

//Singleton class
public class ElasticsearchRestClient implements Managed {

	private Client client;
	
	private static RestHighLevelClient restHighLevelClient;
	
	public ElasticsearchRestClient(final EmployeeDropwizardElasticsearchAppConfiguration config) throws IOException {
		
		//Initialize the settings
		final Settings.Builder settingsBuilder = Settings.builder();
		
		//If a settings file is given, read settings from there
		if (!isNullOrEmpty(config.getSettingsFile())) {
			Path path = Paths.get(config.getSettingsFile());
			
			if(!path.toFile().exists()) {
				try {
					final URL url = Resources.getResource(config.getSettingsFile());
					path = new File(url.toURI()).toPath();					
				} catch (URISyntaxException | NullPointerException e) {
					throw new IllegalArgumentException ("settings file cannot be found", e);
				}
			}
			settingsBuilder.loadFromPath(path);	
		}
		
		//Add any additional user-specific settings
		if(!config.getSettings().isEmpty()) {
			config.getSettings().forEach(settingsBuilder :: put);
		}
		
		final Settings settings = settingsBuilder
				.put("cluster.name", config.getClusterName())
				.build();
		
		//Build a REST client
		if(!config.isTransportClient()) {
			
			HttpHost[] hosts = config.getServers().stream().map(HttpHost::create).toArray(HttpHost[]::new);
			
			RestClientBuilder clientBuilder = RestClient.builder(hosts);
			/*
			if(!config.getHeaders().isEmpty()) {
				Header[] headers = config.getHeaders().entrySet().stream()
						.map(e -> new Basic Header(e.getKey(), e.getValue()))
						.toArray(BasicHeader[]::new);
				clientBuilder.setDefaultHeaders(headers);
			}
			*/
			this.restHighLevelClient = new RestHighLevelClient(clientBuilder);
					
		}
		
		
	}
	
	public ElasticsearchRestClient(Client client) {
        this.client = checkNotNull(client, "Elasticsearch client must not be null");
    }
	
	@Override
    public void start() throws Exception {
    }
	
	@Override
    public void stop() throws Exception {
        closeClient();
        closeRestClient();
    }
	
	
	public Client getClient() {
        return client;
    }
	
	public RestClient getRestClient() {
        return restHighLevelClient.getLowLevelClient();
    }
	
	
	public static RestHighLevelClient getRestHighLevelClient() {
		return restHighLevelClient;
	}
	
	
	private void closeClient() {
        if (null != client) {
            client.close();
        }
    }
	
	private void closeRestClient() throws IOException {
        if (null != restHighLevelClient) {
			restHighLevelClient.close();
        }
    }
	
	/*
	public static RestHighLevelClient getElasticRestClient() {
		try {
			RestHighLevelClient client = null;
	
				System.out.println("host:" + elasticsearchHost + " port:" + elastisearchPort);
		
			client = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticsearchHost, elastisearchPort)));
			
			return client;		
			}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	*/

}
