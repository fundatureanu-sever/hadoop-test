package dev.hadoop.metadata;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import static dev.hadoop.constants.Constants.TAB;
import static dev.hadoop.constants.Constants.EOL;
import static dev.hadoop.constants.Constants.PRICE_COL;
import static dev.hadoop.constants.Constants.CATEGORY_NAME_COL;
import static dev.hadoop.constants.Constants.PRODUCT_ID_COL;

public class ExtendedJDBCMetadataProvider extends AbstractJDBCMetadataProvider {

	public static final String CATEGORIES_META_FILENAME = "categories#categories";
	public static final String PRODUCTS_META_FILENAME = "products#products";

	@Override
	protected String[] processResults(ResultSet results) throws IOException, SQLException {
		FileWriter productsWriter = new FileWriter("products");//productId - categoryId - price 
		FileWriter categoriesWriter = new FileWriter("categories");//categoryId - categoryName
		
		HashMap<String, Integer> categoryMap = new HashMap<String, Integer>();
		int categoryIdCounter = 0;
		
		while (results.next()){
			productsWriter.write(results.getInt(PRODUCT_ID_COL)+TAB);
			
			String categoryName = results.getString(CATEGORY_NAME_COL);
			Integer categoryId = categoryMap.get(categoryName);
			if (categoryId==null){
				categoryId = categoryIdCounter++;
				categoryMap.put(categoryName, categoryId);
				categoriesWriter.write(categoryId+TAB+categoryName+EOL);
			}
			
			productsWriter.write(categoryId+TAB);			
			productsWriter.write(results.getDouble(PRICE_COL)+EOL);
		}	
		
		productsWriter.close();
		categoriesWriter.close();
		
		return new String[]{PRODUCTS_META_FILENAME, CATEGORIES_META_FILENAME};
	}

}
