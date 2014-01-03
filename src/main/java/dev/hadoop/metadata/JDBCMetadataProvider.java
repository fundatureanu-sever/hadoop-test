package dev.hadoop.metadata;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCMetadataProvider implements MetadataProvider {

	private static final int PRICE_COL = 4;
	private static final int CATEGORY_NAME_COL = 3;
	private static final int USER_ID_COL = 1;
	
	//connection details
	public static final String JDBC_HOST = "localhost";
	public static final String JDBC_PORT = "5432";
	private static final String PASSWORD = "p@ssword";
	public static final String USER = "retailer";
	public static final String DB_NAME = "test";
	public static final int N_COLS = 4;
	
	private String metadataFileURI;
	
	public JDBCMetadataProvider() {
		super();
		this.metadataFileURI = METADATA_FILE_NAME;
	}


	@Override
	public String generateMetadata() throws IOException{
		
		try {
			FileWriter writer = new FileWriter(this.metadataFileURI);
		
			String connectionURL = "jdbc:postgresql://"+JDBC_HOST+":"+JDBC_PORT+"/"+DB_NAME;
			Connection conn = DriverManager.getConnection(connectionURL, USER, PASSWORD);		
			String sqlQuery = "SELECT * FROM products";
			PreparedStatement prepStatement = conn.prepareStatement(sqlQuery);
			ResultSet results = prepStatement.executeQuery();
			
			while (results.next()){
				writer.write(results.getInt(USER_ID_COL)+"\t");
				writer.write(results.getString(CATEGORY_NAME_COL)+"\t");
				writer.write(results.getDouble(PRICE_COL)+"\n");
			}			
			
			prepStatement.close();	
			conn.close();
			writer.close();
			
		} catch (SQLException e) {
			throw new IOException(e.getMessage());
		} 
		
		return metadataFileURI;
	}

}
