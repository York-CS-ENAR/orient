package enar.orient;

import java.util.List;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * The Connection class encapsulates the details needed to start
 * an OrientDB server and establish a connection to it.
 * 
 * You won't need to modify this class works, and you don't need
 * to understand how it works unless you're curious. However, 
 * you will probably find it useful to read about the
 * ODatabaseDocumentTx class:
 * http://www.orientdb.org/releases/latest/javadoc/com/orientechnologies/orient/core/db/document/ODatabaseDocumentTx.html
 */
public class Connection {

	private final ODatabaseDocumentTx db;
	
	public Connection(String dbPath) {
		db = new ODatabaseDocumentTx(dbPath);

		if (db.exists()) {
			db.open("admin", "admin");
		} else {
			db.create();
		}
	}
	
	public <RET extends List<?>> RET query(String query) {
		return db.query(new OSQLSynchQuery<ODocument>(query));
	}
	
	public void finalise() {
		db.drop();
		db.close();
	}
}
