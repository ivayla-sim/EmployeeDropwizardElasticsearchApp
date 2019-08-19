package employee.dropwizard.elasticsearch.pck.api;


import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Data;

@Data
public class AggregationBuckets {
	
	String key;
	long docCount;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
	Date minDateOfJoining;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
	Date maxDateOfJoining;
	double avgAge;

}

