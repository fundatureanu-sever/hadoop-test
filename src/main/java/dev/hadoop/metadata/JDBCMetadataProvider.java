package dev.hadoop.metadata;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCMetadataProvider extends AbstractJDBCMetadataProvider {
	
	protected String [] processResults(ResultSet results) throws IOException, SQLException {
		String []metadataFileURIs = new String[]{METADATA_FILENAME_BASE};
		FileWriter writer = new FileWriter(metadataFileURIs[0]);
		while (results.next()){
			writer.write(results.getInt(PRODUCT_ID_COL)+"\t");
			writer.write(results.getString(CATEGORY_NAME_COL)+"\t");
			writer.write(results.getDouble(PRICE_COL)+"\n");
		}		
		writer.close();
		
		return metadataFileURIs;
	}

}
