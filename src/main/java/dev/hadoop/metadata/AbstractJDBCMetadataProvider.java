package dev.hadoop.metadata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class AbstractJDBCMetadataProvider implements MetadataProvider {
	
	//connection details
	public static final String JDBC_HOST = "localhost";
	public static final String JDBC_PORT = "5432";
	private static final String PASSWORD = "p@ssword";
	public static final String USER = "retailer";
	public static final String DB_NAME = "test";
	public static final int N_COLS = 4;
	
	private String[] metadataFileURIs;
	
	public AbstractJDBCMetadataProvider() {
		super();
	}

	@Override
	public String[] generateMetadata() throws IOException{	
		try {
			String connectionURL = "jdbc:postgresql://"+JDBC_HOST+":"+JDBC_PORT+"/"+DB_NAME;
			Connection conn = DriverManager.getConnection(connectionURL, USER, PASSWORD);		
			String sqlQuery = "SELECT * FROM products";
			PreparedStatement prepStatement = conn.prepareStatement(sqlQuery);
			ResultSet results = prepStatement.executeQuery();
			
			this.metadataFileURIs = processResults(results);
			
			prepStatement.close();	
			conn.close();			
			
		} catch (SQLException e) {
			throw new IOException(e.getMessage());
		} 
		
		return metadataFileURIs;
	}

	abstract protected String [] processResults(ResultSet results) throws IOException, SQLException;

}
