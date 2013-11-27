package enar.orient;

import java.util.List;

import com.orientechnologies.orient.core.record.impl.ODocument;

/*
 * This class demonstrates a very simple usage of the 
 * OrientDB document store. Note that this code uses a 
 * single database node. (How might this differ in a 
 * production environment?) 
 * 
 * Run this code using the Orient.launch configuration
 * in Eclipse. (Right-click the .launch file and then select 
 * Run as > Voldemort). 
 * 
 * A decent overview of OrientDB can be found at:
 * https://github.com/orientechnologies/orientdb/wiki/Document-Database
 * 
 * Note that OrientDB is actually a hybrid of a document
 * database and a graph database. Some of the documentation will 
 * be specific to the graph database concepts of OrientDB, but in this
 * seminar we're only considering its document database concepts.
 */
@SuppressWarnings({ "unchecked", "unused" })
public class Orient {

	public static void main(String[] args) {
		new Orient().run();
	}

	// Connect to the orient database. You can (and may need to) change the path
	private Connection connection = new Connection("local:/tmp/db/orient-footballers");
//	private Connection connection = new Connection("local:C:/temp/databases/orient-footballers");
	
	public void run() {
		try {
			// Start by reading this code, running it, changing and rerunning it
			simpleReadAndWrite();

			// Once you're comfortable with the first method, comment in this
			// method, read the source, run it, change and rerun it, etc.
//			inconsistentWrite();

		} finally {
			// This code ensures that we dispose of the connection to the database
			// even if an exception is raised during the execution of
			// our program
			connection.finalise();
		}
	}

	private void simpleReadAndWrite() {
		// Create two initial "FootballTeam" objects
		// which contain nested "Footballer" objects
		ODocument spurs = createFootballTeam(
			"Tottenham Hotspur",
			"White Hart Lane",
			createFootballer("Robert Soldado"),
			createFootballer("Andros Townsend")
		);

		ODocument manUtd = createFootballTeam(
			"Manchester United",
			"Old Trafford",
			createFootballer("Wayne Rooney"),
			createFootballer("Patrice Evra"),
			createFootballer("Tom Cleverley")
		);
		
		// Save both of the objects to the database
		spurs.save();
		manUtd.save();

		// Query the database for a list of all of the objects
		// that are of type "FootballTeam"
		List<ODocument> teams = connection.query("select * from FootballTeam");
		
		// Print out each of the results from the previous query
		for (ODocument t : teams) {
			printFootballTeam(t);
		}
	}
	
	private void inconsistentWrite() {
		// The following code allows us to simulate a potential
		// consistency issue. We'll obtain two copies of the
		// Tottenham Hotspur team from the database, change them 
		// both at the same time and then write them back to the database.
		//
		// In the real world, we'd expect these writes to be 
		// happening on two different clients.
		//
		// Read the code and refer to the OrientDB documentation.
		// What does the documentation say about inconsistent writes?
		//
		// What happens when we run the code? How do we resolve the 
		// problem? What does the OrientDB documentation recommend?
		//
		// What would have happened if the second write had been
		// received by a replica that hadn't yet been notified 
		// of the first write?
		
		ODocument spurs1 = (ODocument)connection.query("select * from FootballTeam").get(0);
		ODocument spurs2 = (ODocument)connection.query("select * from FootballTeam").get(0);
		
		spurs1.field("name", "Spurs");
		spurs2.field("ground", "Wembley");
		
		spurs1.save();
		spurs2.save();
	}

	private ODocument createFootballTeam(String name, String ground, ODocument... players) {
		ODocument doc = new ODocument("FootballTeam");
		doc.field("name", name);
		doc.field("ground", ground);
		doc.field("players", players);
		return doc;
	}

	private ODocument createFootballer(String name) {
		ODocument doc = new ODocument("Footballer");
		doc.field("name", name);
		return doc;
	}
	
	private void printFootballTeam(ODocument team) {
		System.out.println(team.field("name") + " (" + team.field("ground") + ")");

		for (ODocument p : (List<ODocument>) team.field("players")) {
			System.out.println("\t- " + p.field("name"));
		}
	}
}
