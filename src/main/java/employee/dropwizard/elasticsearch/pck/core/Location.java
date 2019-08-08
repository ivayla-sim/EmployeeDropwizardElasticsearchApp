package employee.dropwizard.elasticsearch.pck.core;

import java.math.BigDecimal;
import lombok.Data;

@Data
public class Location {
	
	private BigDecimal lat;
	private BigDecimal lon;

}
