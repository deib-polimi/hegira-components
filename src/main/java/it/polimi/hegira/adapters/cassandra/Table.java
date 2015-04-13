package it.polimi.hegira.adapters.cassandra;

/**
 * This class manages a single Table.
 * Used to check columns, alter the table if necessary, and perform inserts into Cassandra.
 * @author Andrea Celli
 *
 */
public class Table {

	private String tableName;
	
	public Table(String tableName){
		this.tableName=tableName;
	}
}
