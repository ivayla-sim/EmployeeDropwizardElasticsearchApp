package employee.dropwizard.elasticsearch.pck.core;
import lombok.Data;

@Data
public class Addresses {
	
	private int id;
	private String country;
	private String state;
	private String city;
	private String streetNameAndNumber;
	private int zipCode;

}
