package employee.dropwizard.elasticsearch.pck.core;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class EmployeeName {
	
	@NotNull(message = "firstName cannot be null")
	@NotEmpty(message = "firstName cannot be null or empty")
	@NotBlank(message = "firstName cannot be null or whitespace")
	private String firstName;
	
	@NotNull(message = "lastName cannot be null")
	@NotEmpty(message = "lastName cannot be null or empty")
	@NotBlank(message = "lastName cannot be null or whitespace")
	private String lastName;

}
