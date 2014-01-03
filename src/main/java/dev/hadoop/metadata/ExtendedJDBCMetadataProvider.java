package dev.hadoop.metadata;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class ExtendedJDBCMetadataProvider extends AbstractJDBCMetadataProvider {

	@Override
	protected String[] processResults(ResultSet results) throws IOException, SQLException {
		String []metadataFileURIs = new String[]{METADATA_FILENAME_BASE+"_products", 
													METADATA_FILENAME_BASE+"_categories"};
		
		FileWriter productsWriter = new FileWriter(metadataFileURIs[0]);//productId - categoryId - price 
		FileWriter categoriesWriter = new FileWriter(metadataFileURIs[1]);//categoryId - categoryName
		
		HashMap<String, Integer> categoryMap = new HashMap<String, Integer>();
		int categoryIdCounter = 0;
		
		while (results.next()){
			productsWriter.write(results.getInt(PRODUCT_ID_COL)+"\t");
			
			String categoryName = results.getString(CATEGORY_NAME_COL);
			Integer categoryId = categoryMap.get(categoryName);
			if (categoryId==null){
				categoryId = categoryIdCounter++;
				categoryMap.put(categoryName, categoryId);
				categoriesWriter.write(categoryId+"\t"+categoryName);
			}
			
			productsWriter.write(categoryId+"\t");			
			productsWriter.write(results.getDouble(PRICE_COL)+"\n");
		}	
		
		productsWriter.close();
		categoriesWriter.close();
		
		return metadataFileURIs;
	}

}
