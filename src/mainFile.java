import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class mainFile {
	private static String firstString(Scanner sc) {
		String userArgument = null;

		userArgument = sc.nextLine();
		userArgument = userArgument.trim();

		if (userArgument.equalsIgnoreCase("empty")) {
			userArgument = "";
		} else if (userArgument.equalsIgnoreCase("null")) {
			userArgument = null;
		}

		return userArgument;
	}

	public static void main(String[] args) {
		// commands used to add person, media files, attributes of person, media file,
		// record relationship between pair of person
		String addPerson = "addPerson";
		String recordAttr = "recordAttr";
		String recordRef = "recordRef";
		String recordNote = "recordNote";
		String recordChild = "recordChild";
		String recordPartner = "recordPartner";
		String recordDiss = "recordDiss";
		String addMedia = "addMedia";
		String recordMediaAttr = "recordMediaAttr";
		String listPeople = "listPeople";
		String recordTag = "recordTag";
		String locatePerson = "locatePerson";
		String locateFile = "locateFile";
		String findName = "findName";
		String findMediaFile = "findMediaFile";
		String notesRef = "notesRef";
		String occupation = "occupation";
		String findTag = "findTag";
		String findLocation = "findLocation";
		String findIndividuals = "findIndividuals";
		String ancestors = "ancestors";
		String descendents = "descendents";
		String findrelation = "findrelation";
		String findBiologicalMedia = "findBiologicalMedia";
		String quit = "quit";

		// take the input from user
		Scanner sc = new Scanner(System.in);
		String userCommand = "";

		Genealogy g = new Genealogy();
		// by default interface provided to user
		System.out.println("Options available :-");
		System.out.print("(1) " + addPerson + " <Name>");
		System.out.print(" (2) " + recordAttr + " <id>");
		System.out.print(" (3) " + recordRef + " <id> <String>");
		System.out.println(" (4) " + recordNote + " <id> <String>");
		System.out.print("(5) " + recordChild + " <id1> <id2>");
		System.out.print(" (6) " + recordPartner + " <id1> <id2>");
		System.out.println(" (7) " + recordDiss + " <id1> <id2>");
		System.out.println();
		System.out.println("Options for adding File details :-");
		System.out.print("(1) " + addMedia + " <filepath/name>");
		System.out.print(" (2) " + recordMediaAttr + " <id>");
		System.out.print(" (3) " + listPeople + " <id>");
		System.out.println(" (4) " + recordTag + " <id> <String>");
		System.out.println();
		System.out.print("(1) " + locatePerson + " <name>");
		System.out.print(" (2) " + locateFile + " <name>");
		System.out.print(" (3) " + findName + " <id>");
		System.out.print(" (4) " + findMediaFile + " <id>");
		System.out.println(" (5) " + findrelation + " <id1> <id2>");
		System.out.print("(6) " + descendents + " <id> <generations_value>");
		System.out.print(" (7) " + ancestors + " <id> <generations_value>");
		System.out.print(" (8) " + notesRef + " <id>");
		System.out.println(" (9) " + occupation + " <id>");
		System.out.print("(10) " + findTag + " <tag_name> <startDate> <endDate>");
		System.out.println(" (11) " + findLocation + " <location_name> <startDate> <endDate>");
		System.out.print("(12) " + findIndividuals + " <startDate> <endDate>");
		System.out.print(" (13) " + findBiologicalMedia + " <id>");
		System.out.println();
		System.out.println();
		System.out.print("" + quit);

		try {
			do {
				userCommand = sc.next();
				// command used to add name and pass the name to method addPerson
				if (userCommand.equalsIgnoreCase(addPerson)) {
					String name = firstString(sc);
					g.addPerson(name);
				}
				// command used to add attributes for a person and pass to method
				// recordAttributes
				else if (userCommand.equalsIgnoreCase(recordAttr)) {
					int id = sc.nextInt();
					Map<String, String> person_data = new HashMap<String, String>();
					g.recordAttributes(g.objectPerson(id), person_data);
				}
				// command used to add references for a person and pass to method
				// recordReference
				else if (userCommand.equalsIgnoreCase(recordRef)) {
					int id = sc.nextInt();
					String reference = firstString(sc);
					g.recordReference(g.objectPerson(id), reference);
				}
				// command used to add notes for a person and pass to method recordNote
				else if (userCommand.equalsIgnoreCase(recordNote)) {
					int id = sc.nextInt();
					String note = firstString(sc);
					g.recordNote(g.objectPerson(id), note);
				}
				// command used to record child parent relationship and pass pair of objects to
				// method recordChild
				else if (userCommand.equalsIgnoreCase(recordChild)) {
					int id1 = sc.nextInt();
					int id2 = sc.nextInt();
					g.recordChild(g.objectPerson(id1), g.objectPerson(id2));
				}
				// command used to record partnering relationship and pass pair of objects to
				// method recordPartnering
				else if (userCommand.equalsIgnoreCase(recordPartner)) {
					int id1 = sc.nextInt();
					int id2 = sc.nextInt();
					g.recordPartnering(g.objectPerson(id1), g.objectPerson(id2));
				}
				// command used to record partnering relationship and pass pair of objects to
				// method recordDissolution
				else if (userCommand.equalsIgnoreCase(recordDiss)) {
					int id1 = sc.nextInt();
					int id2 = sc.nextInt();
					g.recordDissolution(g.objectPerson(id1), g.objectPerson(id2));
				}
				// command used to add media file and pass to method addMediaFile
				else if (userCommand.equalsIgnoreCase(addMedia)) {
					String file = firstString(sc);
					g.addMediaFile(file);
				}
				// command used to record attributes for a media file
				else if (userCommand.equalsIgnoreCase(recordMediaAttr)) {
					int id = sc.nextInt();
					Map<String, String> file_data = new HashMap<String, String>();
					g.recordMediaAttributes(g.objectFile(id), file_data);
				}
				// command used to add list of people present in a media file
				else if (userCommand.equalsIgnoreCase(listPeople)) {
					int id = sc.nextInt();
					List<PersonIdentity> people = new ArrayList<>();
					g.peopleInMedia(g.objectFile(id), people);
				}
				// command used to add tags for a media file and pass to method tagMedia
				else if (userCommand.equalsIgnoreCase(recordTag)) {
					int id = sc.nextInt();
					String tag = firstString(sc);
					g.tagMedia(g.objectFile(id), tag);
				}
				// command used to locate person in database
				else if (userCommand.equalsIgnoreCase(locatePerson)) {
					String name = firstString(sc);
					g.findPerson(name);
				}
				// command used to locate media file in database
				else if (userCommand.equalsIgnoreCase(locateFile)) {
					String name = firstString(sc);
					g.findMediaFile(name);
				}
				// command used to return name of person from database
				else if (userCommand.equalsIgnoreCase(findName)) {
					int id = sc.nextInt();
					g.findName(g.objectPerson(id));
				}
				// command used to return name of file from database
				else if (userCommand.equalsIgnoreCase(findMediaFile)) {
					int id = sc.nextInt();
					g.findMediaFile(g.objectFile(id));
				}
				// command used to return notes and references added for an individual
				else if (userCommand.equalsIgnoreCase(notesRef)) {
					int id = sc.nextInt();
					g.notesAndReferences(g.objectPerson(id));
				}
				// command used to return occupation added for an individual
				else if (userCommand.equalsIgnoreCase(occupation)) {
					int id = sc.nextInt();
					g.occupation(g.objectPerson(id));
				}
				// command used to return tags added for a media file
				else if (userCommand.equalsIgnoreCase(findTag)) {
					String tag_name = sc.next();
					String startDate = sc.next();
					String endDate = sc.next();
					g.findMediaByTag(tag_name, startDate, endDate);
				}
				// command used to return location added for a media file
				else if (userCommand.equalsIgnoreCase(findLocation)) {
					String location_name = sc.next();
					String startDate = sc.next();
					String endDate = sc.next();
					g.findMediaByLocation(location_name, startDate, endDate);
				}
				// command used to return set of media files that include any of individuals
				// given in the list of people in chronological order
				else if (userCommand.equalsIgnoreCase(findIndividuals)) {
					Set<PersonIdentity> people = new HashSet<>();
					String startDate = sc.next();
					String endDate = sc.next();
					g.findIndividualsMedia(people, startDate, endDate);
				}
				// command use to return all ancestors of given person who are within given
				// generation
				else if (userCommand.equalsIgnoreCase(ancestors)) {
					int id = sc.nextInt();
					int value = sc.nextInt();
					g.ancestores(g.objectPerson(id), value);
				}
				// command use to return all descendents of given person who are within given
				// generation
				else if (userCommand.equalsIgnoreCase(descendents)) {
					int id = sc.nextInt();
					int value = sc.nextInt();
					g.descendents(g.objectPerson(id), value);
				}
				// command use to return relationship between given pair of people and return
				// the degree of cousinship and degree of removal
				else if (userCommand.equalsIgnoreCase(findrelation)) {
					int id1 = sc.nextInt();
					int id2 = sc.nextInt();
					g.findRelation(g.objectPerson(id1), g.objectPerson(id2));
				}
				// command use to return set of media files that include the specified person
				// immediate children in chronological order
				else if (userCommand.equalsIgnoreCase(findBiologicalMedia)) {
					int id = sc.nextInt();
					g.findBiologicalFamilyMedia(g.objectPerson(id));
				} else {
					System.out.println("Bad command: " + userCommand);
				}
			} while (!userCommand.equalsIgnoreCase("quit"));
		} catch (Exception e) {
			// Exception used to let user know if user has not provided data in required
			// format
			System.out.println(e.getMessage());
		}
		sc.close();
	}
}
