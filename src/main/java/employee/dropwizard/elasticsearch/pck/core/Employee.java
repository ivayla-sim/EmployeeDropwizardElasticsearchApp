package employee.dropwizard.elasticsearch.pck.core;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import javax.validation.constraints.Email;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;


import org.hibernate.validator.constraints.Length;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Data;

@Data
public class Employee {
	
	@NotEmpty(message = "Id cannot be null or empty")
	@NotBlank(message = "Id cannot be null or whitespace")
	private String id;
	
	@NotNull(message = "isActive cannot be null")
	private Boolean isActive;
	
	@NotNull(message = "Name cannot be null")
	private EmployeeName name;
	
	@NotNull(message = "Designation cannot be null")
	@NotEmpty(message = "Designation cannot be null or empty")
	private String designation;
	
	@NotNull(message = "DateOfJoining cannot be null")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
	private Date dateOfJoining;
	
	@NotNull(message = "Salary cannot be null")
	//@Min(value = 1000)
	private String salary;
	
	private String picture;
	private int age;
	private String gender;
	private String eyeColor;
	private String company;
	
	@NotNull(message = "Email cannot be null")
	@Pattern(regexp = ".+@.+\\.[a-z]+", message = "Email must match the required pattern - abc123@abc123.abc")
	@Email(message = "Email must be valid")
	private String email;
	
	private String phone;
	private List<Addresses> addresses;
	private Location location;
	
	@Length(min = 0, max = 255, message = "About field must be 0-255 char long")
	private String about;
	
	private List<String> interests;
	private List<Friends> friends;
	private String greeting;
	
	
	

}
