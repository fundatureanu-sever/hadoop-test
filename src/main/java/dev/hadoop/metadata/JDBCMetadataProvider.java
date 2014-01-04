package dev.hadoop.metadata;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static dev.hadoop.constants.Constants.TAB;
import static dev.hadoop.constants.Constants.EOL;

import static dev.hadoop.constants.Constants.PRODUCT_ID_COL;
import static dev.hadoop.constants.Constants.CATEGORY_NAME_COL;
import static dev.hadoop.constants.Constants.PRICE_COL;

public class JDBCMetadataProvider extends AbstractJDBCMetadataProvider {
	
	protected String [] processResults(ResultSet results) throws IOException, SQLException {
		String []metadataFileURIs = new String[]{METADATA_FILENAME_BASE};
		FileWriter writer = new FileWriter(metadataFileURIs[0]);
		while (results.next()){
			writer.write(results.getInt(PRODUCT_ID_COL)+TAB);
			writer.write(results.getString(CATEGORY_NAME_COL)+TAB);
			writer.write(results.getDouble(PRICE_COL)+EOL);
		}		
		writer.close();
		
		return metadataFileURIs;
	}

}
