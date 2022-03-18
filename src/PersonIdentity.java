public class PersonIdentity {
	// declaring id and name
	private int id;
	private String name;
	
	// declaring constructor
	public PersonIdentity(int id, String name) {
		super();
		this.id = id;
		this.name = name;
	}
	// declaring getters and setters 
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
