import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Genealogy {
	Connection connect = null;
	Statement statement = null;
	Statement statement1 = null;
	ResultSet result = null;
	ResultSet resultSet = null;
	ResultSet resultSet1 = null;
	ResultSet resultSet2 = null;
	ResultSet resultSet3 = null;
	Map<Integer, NodeCreation> relations;
	ArrayList<Integer> ancestors = new ArrayList<>();
	ArrayList<Integer> descendents = new ArrayList<>();
	ArrayList<Integer> biological = new ArrayList<>();

	public void connectToDatabase() throws SQLException, ClassNotFoundException {
		// method used to connect with database
		// enter username in place of *****
		String username = "*****";
		// enter passowrd in place of *****
		String password = "*****";
		Class.forName("com.mysql.cj.jdbc.Driver");
		connect = DriverManager.getConnection("jdbc:mysql://localhost:3306?serverTimezone=UTC &  useSSL=false",
				username, password);
		statement = connect.createStatement();
		// enter database name in place of *****
		statement.execute("use *****;");
		statement1 = connect.createStatement();
		// enter database name in place of *****
		statement1.execute("use *****;");
	}

	public void disconnectDatabase() throws SQLException {
		// method used to close the connection with database
		statement.close();
		connect.close();
	}

	// method used to create person object from id provided by user
	public PersonIdentity objectPerson(int id) throws Exception {
		connectToDatabase();
		PersonIdentity pi = null;
		PreparedStatement stmt = connect.prepareStatement("select name from personidentity where id = (" + id + ");");
		resultSet = stmt.executeQuery();
		if (resultSet.next()) {
			pi = new PersonIdentity(id, resultSet.getString("name"));
			resultSet.close();
			disconnectDatabase();
			return pi;
		} else {
			// if invalid person object provided, returns exception
			resultSet.close();
			disconnectDatabase();
			throw new Exception("Person with id " + id + " not present in database");
		}
	}

	// method used to add a person in the table personidentity
	public PersonIdentity addPerson(String name) throws Exception {
		connectToDatabase();
		PersonIdentity pi = null;
		// inserting the person name in table
		statement.executeUpdate("insert into personidentity (name) values ('" + name + "');");
		// fetch the person id once person is added in table
		resultSet = statement.executeQuery("select id from personidentity where name = ('" + name + "');");
		if (resultSet.next()) {
			// create a person object
			int personId = resultSet.getInt("id");
			pi = new PersonIdentity(personId, name);
		}
		resultSet.close();
		disconnectDatabase();
		return pi;
	}

	// method used to record attributes for a given person
	public Boolean recordAttributes(PersonIdentity person, Map<String, String> attributes) throws Exception {
		connectToDatabase();
		// fetching the id from object passed as parameter
		int personId = person.getId();
		// checking whether person with given id is present in database or not
		resultSet = statement.executeQuery("select * from personidentity where id = " + personId + ";");
		if (resultSet.next()) {
			// if person with given id is present
			// checking the value of "Key" and adding corresponding value for that key in
			// table for that person
			String birthdate = null;
			String deathdate = null;
			String gender = null;
			String occupation = null;
			String locationOfbirth = null;
			String locationOfdeath = null;
			if (attributes.containsKey("BirthDate")) {
				birthdate = attributes.get("BirthDate");
			}
			if (attributes.containsKey("DeathDate")) {
				deathdate = attributes.get("DeathDate");
			}
			if (attributes.containsKey("Gender")) {
				// if string provided for gender is in required format which is "m" for male and
				// "f" for female, then only it will be accepted
				if (attributes.get("Gender").equalsIgnoreCase("m") || attributes.get("Gender").equalsIgnoreCase("f")) {
					gender = attributes.get("Gender");
				}
			}
			if (attributes.containsKey("Occupation")) {
				occupation = attributes.get("Occupation");
			}
			if (attributes.containsKey("LocationOfBirth")) {
				locationOfbirth = attributes.get("LocationOfBirth");
			}
			if (attributes.containsKey("LocationOfDeath")) {
				locationOfdeath = attributes.get("LocationOfDeath");
			}
			if (birthdate == null && deathdate == null && gender == null && occupation == null
					&& locationOfbirth == null && locationOfdeath == null) {
				resultSet.close();
				disconnectDatabase();
				return false;
			} else {
				// inserting attributes for given person object
				if (birthdate != null) {
					statement.executeUpdate(
							"update personidentity set birthDate = ('" + birthdate + "') where id = " + personId + ";");
				}
				if (deathdate != null) {
					statement.executeUpdate(
							"update personidentity set deathDate = ('" + deathdate + "') where id = " + personId + ";");
				}
				if (gender != null) {
					statement.executeUpdate(
							"update personidentity set gender = ('" + gender + "') where id = " + personId + ";");
				}
				if (locationOfbirth != null) {
					statement.executeUpdate("update personidentity set locationOfbirth = ('" + locationOfbirth
							+ "') where id = " + personId + ";");
				}
				if (locationOfdeath != null) {
					statement.executeUpdate("update personidentity set locationOfdeath = ('" + locationOfdeath
							+ "') where id = " + personId + ";");
				}
				if (occupation != null) {
					statement.executeUpdate("insert into occupation values (" + personId + ",'" + occupation + "');");
				}
			}
			resultSet.close();
			disconnectDatabase();
			return true;
		}
		// if invalid person object provided, returns false
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method used to add references for given person
	public Boolean recordReference(PersonIdentity person, String reference) throws Exception {
		connectToDatabase();
		// fetching the id from person object passed as parameter
		int personId = person.getId();
		// checking whether person with given id is present in database or not
		resultSet = statement.executeQuery("select * from personidentity where id = " + personId + ";");
		if (resultSet.next()) {
			// storing timestamp at which reference was added for person
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("ddyyyyHHmmss");
			LocalDateTime now = LocalDateTime.now();
			String time = dtf.format(now);
			// if valid id for person is provided, linking the reference details and
			// person id in separate table named reference
			statement.executeUpdate("insert into reference(id, reference, time) values (" + personId + ", '" + reference
					+ "', " + time + ");");
			resultSet.close();
			disconnectDatabase();
			return true;
		}
		// if invalid person object provided, returns false
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method used to add notes for given person
	public Boolean recordNote(PersonIdentity person, String note) throws SQLException, ClassNotFoundException {
		connectToDatabase();
		// fetching the id and name from object passed as parameter
		int personId = person.getId();
		// checking whether person with given id is present in database or not
		resultSet = statement.executeQuery("select * from personidentity where id = " + personId + ";");
		if (resultSet.next()) {
			// storing timestamp at which note was added for person
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("ddyyyyHHmmss");
			LocalDateTime now = LocalDateTime.now();
			String time = dtf.format(now);
			// if valid id for person is provided, linking the note details and
			// person id in separate table named note
			statement.executeUpdate(
					"insert into note(id, notes, time) values (" + personId + ", '" + note + "', " + time + ");");
			resultSet.close();
			disconnectDatabase();
			return true;
		}
		// if invalid person object provided, returns false
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method used to record parent child relationship
	public Boolean recordChild(PersonIdentity parent, PersonIdentity child) throws Exception {
		connectToDatabase();
		// fetching id for first person object which is parent
		int personId1 = parent.getId();
		// checking whether given parent is present in table or not
		resultSet = statement.executeQuery("select * from personidentity where id = " + personId1 + ";");
		if (resultSet.next()) {
			// fetching id for second person object which is child, checking whether given
			// child is present in table or not
			int personId2 = child.getId();
			resultSet1 = statement.executeQuery("select * from personidentity where id = " + personId2 + ";");
			if (resultSet1.next()) {
				// if already two people are present in partner table and having marriage status
				// between them we cannot have parent child relationship between them
				resultSet2 = statement.executeQuery("select * from partner where partner1_id = " + personId1
						+ " and partner2_id = " + personId2 + ";");
				if (!resultSet2.next()) {
					// if already two people are present in dissolution table and having dissolution
					// status between them we cannot have parent child relationship between them
					resultSet3 = statement.executeQuery("select * from dissolution where partner1_id = " + personId1
							+ " and partner2_id = " + personId2 + ";");
					if (!resultSet3.next()) {
						// if no partner or dissolution status is present between given two people then
						// parent-child relationship will be added between them and stored in
						// parentchild table
						statement.executeUpdate("insert into parentchild(parent_id,child_id) values (" + personId1 + ","
								+ personId2 + ");");
						resultSet3.close();
						resultSet2.close();
						resultSet1.close();
						resultSet.close();
						disconnectDatabase();
						return true;
					}
					resultSet3.close();
				}
				resultSet2.close();
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method used to record partnering relationship
	public Boolean recordPartnering(PersonIdentity partner1, PersonIdentity partner2) throws Exception {
		connectToDatabase();
		// fetching id for first person object which is partner1
		int personId1 = partner1.getId();
		// checking whether given person1 is present in table or not
		resultSet = statement.executeQuery("select * from personidentity where id = " + personId1 + ";");
		if (resultSet.next()) {
			// fetching id for second person object which is partner2, and checking whether
			// person2 is present in table or not
			int personId2 = partner2.getId();
			resultSet1 = statement.executeQuery("select * from personidentity where id = " + personId2 + ";");
			if (resultSet1.next()) {
				// if already two person are present in parentchild table and having parent
				// child relationship status between them we cannot have partner
				// relationship between them
				resultSet2 = statement.executeQuery("select * from parentchild where parent_id = " + personId1
						+ " and child_id = " + personId2 + ";");
				if (!resultSet2.next()) {
					// finding year in which they got married
					DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy");
					LocalDateTime now = LocalDateTime.now();
					String year = dtf.format(now);
					// if no parentchild status is present between given two people
					// then partner relationship will be added between them and stored in
					// partner table
					statement.executeUpdate("insert into partner(partner1_id,partner2_id, year) values (" + personId1
							+ "," + personId2 + ", " + year + ");");
					resultSet2.close();
					resultSet1.close();
					resultSet.close();
					disconnectDatabase();
					return true;
				}
				resultSet2.close();
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method used to record dissolution relationship
	public Boolean recordDissolution(PersonIdentity partner1, PersonIdentity partner2)
			throws SQLException, ClassNotFoundException {
		connectToDatabase();
		// fetching id for first person object which is partner1
		int personId1 = partner1.getId();
		// checking whether given person1 is present in table or not
		resultSet = statement.executeQuery("select * from personidentity where id = " + personId1 + ";");
		if (resultSet.next()) {
			// fetching id for second person object which is partner2, and checking whether
			// person2 is present in table or not
			int personId2 = partner2.getId();
			resultSet1 = statement.executeQuery("select * from personidentity where id = " + personId2 + ";");
			if (resultSet1.next()) {
				// if already two person are present in parentchild table and having parent
				// child relationship status between them we cannot have dissolution
				// relationship between them
				resultSet2 = statement.executeQuery("select * from parentchild where parent_id = " + personId1
						+ " and child_id = " + personId2 + ";");
				if (!resultSet2.next()) {
					// finding year in which they got divorced
					DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy");
					LocalDateTime now = LocalDateTime.now();
					String year = dtf.format(now);
					// if no parentchild relationship is present between given two people then
					// dissolution relationship will be added between them and stored in
					// dissolution table
					statement.executeUpdate("insert into dissolution(partner1_id,partner2_id,year) values (" + personId1
							+ "," + personId2 + ", " + year + ");");
					resultSet2.close();
					resultSet1.close();
					resultSet.close();
					disconnectDatabase();
					return true;
				}
				resultSet2.close();
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method used to create file object from id provided by user
	public FileIdentifier objectFile(int id) throws Exception {
		connectToDatabase();
		FileIdentifier fi = null;
		PreparedStatement stmt = connect.prepareStatement("select file from fileidentity where id = (" + id + ");");
		resultSet = stmt.executeQuery();
		if (resultSet.next()) {
			fi = new FileIdentifier(id, resultSet.getString("file"));
			resultSet.close();
			disconnectDatabase();
			return fi;
		} else {
			// if invalid file object provided, returns exception
			resultSet.close();
			disconnectDatabase();
			throw new Exception("File with id " + id + " not present in database");
		}
	}

	// Method used to add media file
	public FileIdentifier addMediaFile(String fileLocation) throws Exception {
		connectToDatabase();
		FileIdentifier fi = null;
		// insert file in table named fileidentity
		statement.executeUpdate("insert into fileidentity (file) values ('" + fileLocation + "');");
		// fetch the id once file is added
		resultSet = statement.executeQuery("select id from fileidentity where file = ('" + fileLocation + "');");
		while (resultSet.next()) {
			// create an object
			int fileId = resultSet.getInt("id");
			fi = new FileIdentifier(fileId, fileLocation);
		}
		resultSet.close();
		disconnectDatabase();
		return fi;
	}

	// method used to record attributes for a file
	public Boolean recordMediaAttributes(FileIdentifier fileIdentifier, Map<String, String> attributes) throws Exception {
		connectToDatabase();
		// fetching id from file object passed as parameter
		int fileId = fileIdentifier.getFileId();
		// checking whether file with given id present in table or not
		resultSet = statement.executeQuery("select * from fileidentity where id = " + fileId + ";");
		if (resultSet.next()) {
			// if file is present
			// checking the value of "Key" and add corresponding value for that key in table
			// for that file
			String date = null;
			String location = null;
			if (attributes.containsKey("Date")) {
				date = attributes.get("Date");
			}
			if (attributes.containsKey("Location")) {
				location = attributes.get("Location");
			}
			if (date == null && location == null) {
				resultSet.close();
				disconnectDatabase();
				return false;
			} else {
				// inserting attributes for given file object
				if (date != null) {
					statement.executeUpdate(
							"update fileidentity set date = ('" + date + "') where id = " + fileId + ";");
				}
				if (location != null) {
					statement.executeUpdate(
							"update fileidentity set location = ('" + location + "') where id = " + fileId + ";");
				}
			}
			resultSet.close();
			disconnectDatabase();
			return true;
		}
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// Method used to add list of people for a given file
	public Boolean peopleInMedia(FileIdentifier fileIdentifier, List<PersonIdentity> people) throws Exception {
		connectToDatabase();
		// fetching the id and name from file object
		int fileId = fileIdentifier.getFileId();
		// checking whether file with given id is present in database or not
		resultSet = statement.executeQuery("select * from fileidentity where id = " + fileId + ";");
		if (resultSet.next()) {
			// fetching each id for person from List people
			for (int i = 0; i < people.size(); i++) {
				// if valid id for person is provided whose data is present in personidentity
				// table then only that person will be linked to given media file
				resultSet1 = statement
						.executeQuery("select * from personidentity where id = " + people.get(i).getId() + ";");
				if (resultSet1.next()) {
					// linking the media file and person by fetching respective ids of file and
					// person and adding data in separate table peoplemedia
					statement.executeUpdate("insert into peoplemedia(file_id,peoplepresent_id) values (" + fileId + ", "
							+ people.get(i).getId() + ");");
				}
			}
			resultSet1.close();
			resultSet.close();
			disconnectDatabase();
			return true;
		}
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method used to add tags for given media file
	public Boolean tagMedia(FileIdentifier fileIdentifier, String tag) throws SQLException, ClassNotFoundException {
		connectToDatabase();
		// fetching id and name from file object
		int fileId = fileIdentifier.getFileId();
		// checking whether file with given id is present in database or not
		resultSet = statement.executeQuery("select * from fileidentity where id = " + fileId + ";");
		if (resultSet.next()) {
			// inserting tag for a given media file by linking the file id and tag in
			// separate table named tag
			statement.executeUpdate("insert into tag(id, tags) values (" + fileId + ", '" + tag + "');");
			resultSet.close();
			disconnectDatabase();
			return true;
		}
		resultSet.close();
		disconnectDatabase();
		return false;
	}

	// method use to locate person in database
	public PersonIdentity findPerson(String name) throws Exception {
		connectToDatabase();
		PersonIdentity pi;
		resultSet = statement.executeQuery("select id from personidentity where name = ('" + name + "');");
		// if person name is present return the object else returns null
		if (resultSet.next()) {
			pi = new PersonIdentity(resultSet.getInt("id"), name);
			resultSet.close();
			disconnectDatabase();
			return pi;
		}
		resultSet.close();
		disconnectDatabase();
		return null;
	}

	// method use to locate media file in database
	public FileIdentifier findMediaFile(String name) throws Exception {
		connectToDatabase();
		FileIdentifier fi;
		resultSet = statement.executeQuery("select id from fileidentity where file = ('" + name + "');");
		// if file name is present return the object else returns null
		if (resultSet.next()) {
			int id = resultSet.getInt("id");
			fi = new FileIdentifier(id, name);
			resultSet.close();
			disconnectDatabase();
			return fi;
		}
		resultSet.close();
		disconnectDatabase();
		return null;
	}

	// method used to return the name for person whose id is passed
	public String findName(PersonIdentity id) throws Exception {
		connectToDatabase();
		int p_id = id.getId();
		resultSet = statement.executeQuery("select name from personidentity where id = (" + p_id + ");");
		// if id is present in db returns the name assigned to it else returns null
		if (resultSet.next()) {
			String name = resultSet.getString("name");
			resultSet.close();
			disconnectDatabase();
			return name;
		}
		resultSet.close();
		disconnectDatabase();
		return null;
	}

	// method used to return the name for media file whose id is passed
	public String findMediaFile(FileIdentifier fileId) throws Exception {
		connectToDatabase();
		int f_id = fileId.getFileId();
		resultSet = statement.executeQuery("select file from fileidentity where id = (" + f_id + ");");
		// if id is present in db return the name assigned to it else returns null
		if (resultSet.next()) {
			String file_name = resultSet.getString("file");
			resultSet.close();
			disconnectDatabase();
			return file_name;
		}
		resultSet.close();
		disconnectDatabase();
		return null;
	}

	// method used to return all notes and references allocated to given person
	// object
	public List<String> notesAndReferences(PersonIdentity person) throws Exception {
		connectToDatabase();
		List<String> notesRefs = new ArrayList<>();
		int p_id = person.getId();
		// query used to check whether object with given id is present in personidentity
		// table or not, if not returns null
		resultSet = statement.executeQuery("select id from personidentity where id = (" + p_id + ");");
		if (resultSet.next()) {
			// if present, check if any notes, references are assigned to given person in
			// table note and reference, if yes, fetch them in order of time in which they
			// were added
			resultSet1 = statement.executeQuery("select notes as data, time from note where note.id = (" + p_id
					+ ")  union select reference as data, time from reference where reference.id = (" + p_id
					+ ") order by time;");
			while (resultSet1.next()) {
				notesRefs.add(resultSet1.getString("data"));
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return notesRefs;
	}

	// method used to return all occupations allocated to given person
	public List<String> occupation(PersonIdentity person) throws Exception {
		connectToDatabase();
		List<String> occ = new ArrayList<>();
		int p_id = person.getId();
		// query used to check whether object with given id is present in personidentity
		// table or not, if not returns null
		resultSet = statement.executeQuery("select id from personidentity where id = (" + p_id + ");");
		if (resultSet.next()) {
			// if present, check if any occupations are assigned to given person in table
			// occupation
			resultSet1 = statement.executeQuery("select occupation from occupation where id = (" + p_id + ");");
			while (resultSet1.next()) {
				occ.add(resultSet1.getString("occupation"));
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return occ;
	}

	// method used to return all files having given tag between given
	// time period
	public Set<FileIdentifier> findMediaByTag(String tag, String startDate, String endDate) throws Exception {
		connectToDatabase();
		Set<FileIdentifier> tags = new HashSet<>();
		FileIdentifier fi;
		// used to validate whether dates are provided in given format or not
		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
		sd.setLenient(false);
		if (startDate != null) {
			sd.parse(startDate);
		}
		if (endDate != null) {
			sd.parse(endDate);
		}
		// query used to check is there any files with given tag in table tag, if not
		// returns null
		PreparedStatement stmt = connect.prepareStatement("select * from tag where tags = ('" + tag + "');");
		resultSet = stmt.executeQuery();
		while (resultSet.next()) {
			int file_id = resultSet.getInt("id");
			// if present, fetch the files names and check the dates for each file
			if (startDate != null && endDate != null) {
				// query used to fetch files with given tag within given start and end date
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") and date between ('" + startDate + "') and ('" + endDate
						+ "') union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					tags.add(fi);
				}
			} else if (startDate == null && endDate != null) {
				// query used to fetch files with given tag when start date is null
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") and date <= ('" + endDate
						+ "') union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					tags.add(fi);
				}
			} else if (startDate != null && endDate == null) {
				// query used to fetch files with given tag when end date is null
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") and date >= ('" + startDate
						+ "') union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					tags.add(fi);
				}
			} else if (startDate == null && endDate == null) {
				// query used to fetch files with given tag when both start date and end date
				// are null
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					tags.add(fi);
				}
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return tags;
	}

	// method used to return all files having given location between given
	// time period
	public Set<FileIdentifier> findMediaByLocation(String location, String startDate, String endDate) throws Exception {
		connectToDatabase();
		Set<FileIdentifier> locations = new HashSet<>();
		FileIdentifier fi;
		// used to validate whether dates are provided in given format or not
		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
		sd.setLenient(false);
		if (startDate != null) {
			sd.parse(startDate);
		}
		if (endDate != null) {
			sd.parse(endDate);
		}
		// query used to check is there any files with given location in table
		// fileidentity, if not returns null
		PreparedStatement stmt = connect
				.prepareStatement("select * from fileidentity where location = ('" + location + "');");
		resultSet = stmt.executeQuery();
		while (resultSet.next()) {
			int file_id = resultSet.getInt("id");
			// if present, fetch the files names and check the dates for each file
			if (startDate != null && endDate != null) {
				// query used to fetch files with given location within given start and end date
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") and date between ('" + startDate + "') and ('" + endDate
						+ "') union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					locations.add(fi);
				}
			} else if (startDate == null && endDate != null) {
				// query used to fetch files with given location when start date is null
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") and date <= ('" + endDate
						+ "') union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					locations.add(fi);
				}
			} else if (startDate != null && endDate == null) {
				// query used to fetch files with given location when end date is null
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") and date >= ('" + startDate
						+ "') union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					locations.add(fi);
				}
			} else if (startDate == null && endDate == null) {
				// query used to fetch files with given location when both start date and end
				// date are null
				resultSet1 = statement.executeQuery("select file from fileidentity where id = (" + file_id
						+ ") union select file from fileidentity where date is null and id = (" + file_id + ");");
				if (resultSet1.next()) {
					fi = new FileIdentifier(file_id, resultSet1.getString("file"));
					locations.add(fi);
				}
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return locations;
	}

	// method used to return set of media files that include any of individuals
	// given in the list of people between given time period
	public List<FileIdentifier> findIndividualsMedia(Set<PersonIdentity> people, String startDate, String endDate)
			throws Exception {
		connectToDatabase();
		List<FileIdentifier> individuals = new ArrayList<>();
		ArrayList<Integer> persons = new ArrayList<>();
		FileIdentifier fi;
		// used to validate whether dates are provided in given format or not
		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
		sd.setLenient(false);
		if (startDate != null) {
			sd.parse(startDate);
		}
		if (endDate != null) {
			sd.parse(endDate);
		}
		// fetching ids of each person and storing it in arraylist persons
		Iterator<PersonIdentity> iterator = people.iterator();
		while (iterator.hasNext()) {
			PersonIdentity data = iterator.next();
			int id = data.getId();
			persons.add(id);
		}
		// query used to check is there any person with id in table personidentity, if
		// not returns no media files for that person and checks media files for another
		// person in list till all media files are returned for all people present in
		// list

		// query used to fetch files within given start and end date
		if (startDate != null && endDate != null) {
			String sql = "select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date between '"
					+ startDate + "' and '" + endDate
					+ "' union select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date is null order by case when date is null then 1 else 0 end, date";
			// fetching all ids from arraylist persons and replacing them in place of (?) in
			// sql query
			String ids = persons.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(",", "(", ")"));
			sql = sql.replace("(?)", ids);
			PreparedStatement stmt = connect.prepareStatement(sql);
			result = stmt.executeQuery();
			while (result.next()) {
				// adding files found in list individuals of type FileIdentifier
				fi = new FileIdentifier(result.getInt("id"), result.getString("file"));
				individuals.add(fi);
			}
		}

		// query used to fetch files when start date is null
		else if (startDate == null && endDate != null) {
			String sql = "select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date <= '"
					+ endDate
					+ "' union select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date is null order by case when date is null then 1 else 0 end, date";
			String ids = persons.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(",", "(", ")"));
			sql = sql.replace("(?)", ids);
			PreparedStatement stmt = connect.prepareStatement(sql);
			result = stmt.executeQuery();
			while (result.next()) {
				fi = new FileIdentifier(result.getInt("id"), result.getString("file"));
				individuals.add(fi);
			}
		}

		// query used to fetch files when end date is null
		else if (startDate != null && endDate == null) {
			String sql = "select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date >= '"
					+ startDate
					+ "' union select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date is null order by case when date is null then 1 else 0 end, date";
			String ids = persons.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(",", "(", ")"));
			sql = sql.replace("(?)", ids);
			PreparedStatement stmt = connect.prepareStatement(sql);
			result = stmt.executeQuery();
			while (result.next()) {
				fi = new FileIdentifier(result.getInt("id"), result.getString("file"));
				individuals.add(fi);
			}
		}

		// query used to fetch files when both start date and end date is null
		else if (startDate == null && endDate == null) {
			String sql = "select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) union select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date is null order by case when date is null then 1 else 0 end, date";
			String ids = persons.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(",", "(", ")"));
			sql = sql.replace("(?)", ids);
			PreparedStatement stmt = connect.prepareStatement(sql);
			result = stmt.executeQuery();
			while (result.next()) {
				fi = new FileIdentifier(result.getInt("id"), result.getString("file"));
				individuals.add(fi);
			}
		}
		result.close();
		disconnectDatabase();
		return individuals;
	}

	// method use to create nodes and create n-ary tree from nodes for each parent
	// child relationship present in table parentchild
	private void createTree() throws SQLException, ClassNotFoundException {
		List<NodeCreation> nodes = new ArrayList<>();
		resultSet = statement.executeQuery("select * from parentchild");
		while (resultSet.next()) {
			// creating nodes for each parent child relationship
			NodeCreation node = new NodeCreation(resultSet.getInt("parent_id"), resultSet.getInt("child_id"));
			nodes.add(node);
		}
		// creating n-ary tree from the nodes
		relations = new HashMap<>();
		// saving all nodes created to a map named relations
		for (NodeCreation currentChild : nodes) {
			relations.put(currentChild.getChildId(), currentChild);
		}
		// looping and assigning relationship between parent child
		for (NodeCreation currentChild : nodes) {
			int parentId = currentChild.getParentId();
			if (parentId != 0) {
				NodeCreation prevParent = relations.get(parentId);
				if (prevParent != null) {
					currentChild.setParent(prevParent);
					prevParent.addChild(currentChild);
					relations.put(parentId, prevParent);
					relations.put(currentChild.getChildId(), currentChild);
				}
			}
		}
	}

	// method used to find immediate ancestors of a child and looping till all
	// ancestors are fetched till level of tree equals generations provided
	private void parents(int id, int level, int generations) throws SQLException {
		Iterator<Map.Entry<Integer, NodeCreation>> iterator = relations.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, NodeCreation> data = iterator.next();
			if (id == data.getValue().getChildId() && level <= generations) {
				// getting the immediate ancestors of given child
				resultSet = statement.executeQuery("select parent_id from parentchild where child_id = (" + id + ");");
				while (resultSet.next()) {
					int parent_id = resultSet.getInt("parent_id");
					ancestors.add(parent_id);
					// if level is not equal to generations then recursively calling method and
					// finding next set of ancestors in level above, repeating this process till
					// level and generations value becomes same
					if (level != generations && generations > 1) {
						Iterator<Map.Entry<Integer, NodeCreation>> iterator1 = relations.entrySet().iterator();
						while (iterator1.hasNext()) {
							Map.Entry<Integer, NodeCreation> data1 = iterator1.next();
							// considering parent found above as new child and finding its ancestors
							if (parent_id == data1.getValue().getChildId()) {
								int parent_id1 = data1.getValue().getParentId();
								// adding all ancestors found in arraylist
								ancestors.add(parent_id1);
								// incrementing level for next recursively call
								parents(parent_id1, level + 2, generations);
								resultSet1 = statement1.executeQuery(
										"select parent_id from parentchild where child_id = (" + parent_id + ");");
								while (resultSet1.next()) {
									int guardian = resultSet1.getInt("parent_id");
									if (!ancestors.contains(guardian)) {
										ancestors.add(guardian);
										// calling recursively method parents
										parents(guardian, level + 2, generations);
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// method used to return the ancestors found for given person object
	public Set<PersonIdentity> ancestores(PersonIdentity person, Integer generations) throws Exception {
		connectToDatabase();
		ancestors.clear();
		createTree();
		Set<PersonIdentity> ancestors_found = new HashSet<>();
		PersonIdentity pi;
		// checking whether person object provided is present in parentchild table or
		// not
		PreparedStatement stmt = connect
				.prepareStatement("select * from parentchild where child_id = (" + person.getId() + ");");
		resultSet = stmt.executeQuery();
		// initializing level of tree as 1
		int count_level = 1;
		if (resultSet.next()) {
			// calling method parents to find ancestors for given person object
			parents(person.getId(), count_level, generations);
		}
		// once ancestors found, returning all ancestors as set of type personidentity
		for (int i = 0; i < ancestors.size(); i++) {
			int p_id = ancestors.get(i);
			resultSet1 = statement.executeQuery("select name from personidentity where id = (" + p_id + ");");
			if (resultSet1.next()) {
				pi = new PersonIdentity(p_id, resultSet1.getString("name"));
				ancestors_found.add(pi);
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return ancestors_found;
	}

	// method used to find immediate children of a child and looping till all
	// descendents are fetched till level of tree equals generations provided
	private void childs(int id, int level, int generations) throws SQLException {
		Iterator<Map.Entry<Integer, NodeCreation>> iterator = relations.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, NodeCreation> data = iterator.next();
			if (level <= generations) {
				// getting the immediate children of given child
				resultSet = statement.executeQuery("select child_id from parentchild where parent_id = (" + id + ");");
				while (resultSet.next()) {
					int child_id = resultSet.getInt("child_id");
					// adding all descendents found in arraylist
					descendents.add(child_id);
					if(resultSet.isLast() && generations == 1) {
						return;
					}
					// if level is not equal to generations then recursively calling method and
					// finding next set of descendents in level below, repeating this process till
					// level and generations value becomes same
					if (level != generations && generations > 1) {
						Iterator<Map.Entry<Integer, NodeCreation>> iterator1 = relations.entrySet().iterator();
						while (iterator1.hasNext()) {
							Map.Entry<Integer, NodeCreation> data1 = iterator1.next();
							// considering child found above as new parent and finding its descendents
							if (child_id == data1.getValue().getParentId()) {
								int child_id1 = data1.getValue().getChildId();
								descendents.add(child_id1);
								resultSet1 = statement1.executeQuery(
										"select child_id from parentchild where parent_id = (" + child_id1 + ");");
								while (resultSet1.next()) {
									int guardian = resultSet1.getInt("child_id");
									if (!descendents.contains(guardian) && level + 2 <= generations) {
										descendents.add(guardian);
										// calling recursively method parents
										childs(guardian, level + 3, generations);
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// method used to return the descendents found for given person object
	public Set<PersonIdentity> descendents(PersonIdentity person, Integer generations) throws Exception {
		connectToDatabase();
		descendents.clear();
		createTree();
		Set<PersonIdentity> descendents_found = new HashSet<>();
		PersonIdentity pi;
		// checking whether person object provided is present in parentchild table or
		// not
		PreparedStatement stmt = connect
				.prepareStatement("select * from parentchild where parent_id = (" + person.getId() + ");");
		resultSet = stmt.executeQuery();
		// initializing level of tree as 1
		int count_level = 1;
		if (resultSet.next()) {
			childs(person.getId(), count_level, generations);
		}
		Set<Integer> set = new HashSet<>(descendents);
		descendents.clear();
		descendents.addAll(set);
		// once descendents found, returning all descendents as set of type
		// personidentity
		for (int i = 0; i < descendents.size(); i++) {
			int p_id = descendents.get(i);
			resultSet1 = statement.executeQuery("select name from personidentity where id = (" + p_id + ");");
			if (resultSet1.next()) {
				pi = new PersonIdentity(p_id, resultSet1.getString("name"));
				descendents_found.add(pi);
			}
			resultSet1.close();
		}
		resultSet.close();
		disconnectDatabase();
		return descendents_found;
	}

	// method used to return media files for all immediate children for a given
	// person object
	public List<FileIdentifier> findBiologicalFamilyMedia(PersonIdentity person) throws Exception {
		connectToDatabase();
		biological.clear();
		createTree();
		List<FileIdentifier> files = new ArrayList<>();
		FileIdentifier fi;
		// checking whether given person object present in personidentity table or not
		PreparedStatement stmt = connect
				.prepareStatement("select * from personidentity where id = (" + person.getId() + ");");
		resultSet = stmt.executeQuery();
		if (resultSet.next()) {
			// query used to find immediate children
			resultSet = statement.executeQuery("select child_id from parentchild where parent_id = (" + person.getId() + ");");
			while (resultSet.next()) {
				biological.add(resultSet.getInt("child_id"));
			}
			// once ids for immediate children found, checking whether any file is linked to
			// given child in table peoplemedia, repeating this process till all children
			// and files allocated to them are fetched
			String sql = "select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) union select id, file, date from fileidentity where id in (select file_id from peoplemedia where peoplepresent_id in (?)) and date is null order by case when date is null then 1 else 0 end, date;";
			// fetching all ids from arraylist biological and replacing them in place of (?)
			// in sql query
			String ids = biological.stream().map(x -> String.valueOf(x)).collect(Collectors.joining(",", "(", ")"));
			sql = sql.replace("(?)", ids);
			if (biological.size() != 0) {
				PreparedStatement stmt1 = connect.prepareStatement(sql);
				resultSet1 = stmt1.executeQuery();
				while (resultSet1.next()) {
					// adding files found in list named files of type FileIdentifier
					fi = new FileIdentifier(resultSet1.getInt("id"), resultSet1.getString("file"));
					files.add(fi);
				}
				resultSet1.close();
			}
		}
		resultSet.close();
		disconnectDatabase();
		return files;
	}

	// method used to find all ancestors for given person id
	private void allParents(int id) throws SQLException {
		Iterator<Map.Entry<Integer, NodeCreation>> iterator = relations.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, NodeCreation> data = iterator.next();
			if (id == data.getValue().getChildId()) {
				resultSet = statement.executeQuery("select parent_id from parentchild where child_id = (" + id + ");");
				while (resultSet.next()) {
					int guardian = resultSet.getInt("parent_id");
					if (!ancestors.contains(guardian)) {
						ancestors.add(guardian);
						allParents(guardian);
					}
				}
			}
		}
	}

	// declaring variables
	// countParent is used for storing height of a node in tree
	int countParent = 0;
	boolean foundParent = false;

	// method used to find height of node from common parent node
	private int findHeight(int id, int common) throws SQLException {
		Iterator<Map.Entry<Integer, NodeCreation>> iterator = relations.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, NodeCreation> data = iterator.next();
			if (id == data.getValue().getChildId()) {
				resultSet = statement.executeQuery("select parent_id from parentchild where child_id = (" + id + ");");
				boolean loop = false;
				while (resultSet.next()) {
					int parent_id = resultSet.getInt("parent_id");
					if (parent_id == common) {
						// if parent found which is equal to common parent
						if (loop == false) {
							countParent++;
						}
						foundParent = true;
						break;
					} else {
						// repeat till parent node is not equal to common root node
						if (foundParent == false) {
							countParent++;
							loop = true;
							findHeight(parent_id, common);
						}
					}
				}
			}
		}
		return 0;
	}

	// declaring arraylists to store ancestors for each node
	// parents1 is used to store all ancestors for person1
	ArrayList<Integer> parents1 = new ArrayList<>();
	// parents2 is used to store all ancestors for person2
	ArrayList<Integer> parents2 = new ArrayList<>();
	// arraylist used to store common ancestors for both person1 and person2
	ArrayList<Integer> commonparents = new ArrayList<>();
	// variables declared to store height of each specific object from common parent
	// node, count1 stores height for person1, count2 stores height for
	// person2
	int count1 = 0, count2 = 0;

	// method returns height of each both persons
	private void relation(int id1, int id2) throws SQLException {
		// calculating all ancestors for person1
		allParents(id1);
		parents1.add(id1);
		parents1.addAll(ancestors);
		ancestors.clear();

		// calculating all ancestors for person2
		allParents(id2);
		parents2.add(id2);
		parents2.addAll(ancestors);
		ancestors.clear();

		// storing common ancestors in commonparents arraylist
		commonparents = new ArrayList<Integer>(parents2);
		commonparents.retainAll(parents1);
		if (commonparents.size() != 0) {
			// if ancestors list of person2 contains person1 as a ancestor then count1 will
			// be zero as person1 is ancestor for person2 and height will be calculated for
			// person2 from person1 and will be stored in count2
			if (parents2.contains(id1)) {
				count1 = 0;
				findHeight(id2, id1);
				count2 = countParent;
				countParent = 0;
				foundParent = false;
				return;
			}
			// if ancestors list of person1 contains person2 as a ancestor then count2 will
			// be zero as person2 is ancestor for person1 and height will be calculated for
			// person1 from person2 and will be stored in count1
			else if (parents1.contains(id2)) {
				count2 = 0;
				findHeight(id1, id2);
				count1 = countParent;
				countParent = 0;
				foundParent = false;
				return;
			} else {
				// if both person1 and person2 are not present in each others ancestors list
				// then common ancestors will be found and first encountered common parent will
				// be considered and height will be calculated for person1 and person2 from that
				// parent and will be stored in count1 and count2 respectively
				int common = commonparents.get(0);
				findHeight(id1, common);
				count1 = countParent;
				countParent = 0;
				foundParent = false;
				findHeight(id2, common);
				count2 = countParent;
				countParent = 0;
				foundParent = false;
				return;
			}
		} else {
			// if no common parent return
			return;
		}
	}

	public BiologicalRelation findRelation(PersonIdentity person1, PersonIdentity person2) throws Exception {
		connectToDatabase();
		ancestors.clear();
		parents1.clear();
		parents2.clear();
		commonparents.clear();
		// declaring initial value of height as zero
		countParent = 0;
		foundParent = false;
		createTree();
		BiologicalRelation br = null;
		int degreeOfcousinship = Integer.MAX_VALUE, degreeOfremoval = Integer.MAX_VALUE;
		// query is used to check person1 with given id present in personidentity table
		// or not
		PreparedStatement stmt = connect
				.prepareStatement("select * from personidentity where id = (" + person1.getId() + ");");
		resultSet = stmt.executeQuery();
		// query is used to check person2 with given id present in personidentity table
		// or not
		PreparedStatement stmt1 = connect
				.prepareStatement("select * from personidentity where id = (" + person2.getId() + ");");
		resultSet1 = stmt1.executeQuery();
		if (resultSet.next() && resultSet1.next()) {
			// if both person1 and person2 are present find their common parent and heights
			relation(person1.getId(), person2.getId());
			// calculating degree of cousinship and removal
			if (commonparents.size() != 0) {
				degreeOfcousinship = Math.min(count1, count2) - 1;
				degreeOfremoval = Math.abs(count1 - count2);
			}
			// returning object of class BiologicalRelation
			br = new BiologicalRelation(degreeOfcousinship, degreeOfremoval);
		}
		resultSet1.close();
		resultSet.close();
		disconnectDatabase();
		return br;
	}
}
