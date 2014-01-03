package dev.hadoop.v1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dev.hadoop.metadata.MetadataProvider;

public class ReportByUserIdReducer extends Reducer<IntWritable, IntermediateUserInfo, NullWritable, Text>{

	public static final int PRODUCTS_FILE_HEADER_LENGTH = 3;

	private HashMap<Integer, ProductInfo> productMap = new HashMap<Integer, ProductInfo>();

	private Calendar calendar = Calendar.getInstance();
	private CategoryQuantPair tempPair = new CategoryQuantPair("", 0);

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		//TODO use in distributed mode String productsFileName = context.getConfiguration().get("mapred.cache.files");
		String productsFileName = MetadataProvider.METADATA_FILE_NAME;
		BufferedReader productsReader = new BufferedReader(new FileReader(productsFileName));

		String line;
		while ((line = productsReader.readLine()) != null) {
			String[] tokens = line.split("\\t");
			if (tokens.length!=PRODUCTS_FILE_HEADER_LENGTH){
				continue;
			}
			int productId = Integer.parseInt(tokens[0]);
			String categoryName = tokens[1];
			double productPrice = Double.parseDouble(tokens[2]);
			productMap.put(productId, new ProductInfo(categoryName, productPrice));
		}

		productsReader.close();
	}

	@Override
	public void reduce(IntWritable userId, Iterable<IntermediateUserInfo> userInfoCollection, Context context) throws IOException, InterruptedException {
		double[] qRevenues = new double[4];

		TreeMap<CategoryQuantPair, Integer> categorySortedMap = new TreeMap<CategoryQuantPair, Integer>();

		for (IntermediateUserInfo userInfo : userInfoCollection) {
			ProductInfo product = productMap.get(userInfo.productId);
			if (product == null){
				System.err.println(userInfo.productId+" was not found in the metadata; computing report for user "+userId.get()+" without this product");
				continue;
			}
			
			updateRevenues(qRevenues, userInfo, product);

			updateCategorySortedMap(categorySortedMap, userInfo, product);
		}

		String outputLine = buildOutputLine(userId, qRevenues, categorySortedMap);
		context.write(NullWritable.get(), new Text(outputLine));
	}

	private String buildOutputLine(IntWritable userId, double[] qRevenues, TreeMap<CategoryQuantPair, Integer> sortedMap) {
		StringBuilder output = new StringBuilder(""+userId.get());
		output.append("\t");
		output.append(sortedMap.firstKey().categoryName);
		for (int i = 0; i < qRevenues.length; i++) {
			output.append("\t");
			if (qRevenues[i]==0){
				output.append('-');
			}
			else{
				output.append(qRevenues[i]);
			}
		}
		return output.toString();
	}

	private void updateCategorySortedMap(TreeMap<CategoryQuantPair, Integer> sortedMap, IntermediateUserInfo userInfo, ProductInfo product) {
		tempPair.categoryName = product.catName;
		Integer quant = sortedMap.get(tempPair);
		if (quant == null) {
			sortedMap.put(new CategoryQuantPair(product.catName, userInfo.quantity), userInfo.quantity);
		} else {
			Integer prevQuant = sortedMap.remove(tempPair);
			int newQuantity = userInfo.quantity + prevQuant;
			sortedMap.put(new CategoryQuantPair(product.catName, newQuantity), newQuantity);
		}
	}

	private void updateRevenues(double[] qRevenues, IntermediateUserInfo userInfo, ProductInfo product) {
		calendar.setTime(userInfo.date);
		int month = calendar.get(Calendar.MONTH);

		double price = product.price;
		qRevenues[month / 3] += price * userInfo.quantity;
	}

	static class CategoryQuantPair implements Comparable<CategoryQuantPair>{
		String categoryName;
		int quantity;
		
		public CategoryQuantPair(String categoryName, int quantity) {
			super();
			this.categoryName = categoryName;
			this.quantity = quantity;
		}

		@Override
		public int hashCode() {
			return categoryName.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			CategoryQuantPair pair = (CategoryQuantPair)obj;
			return categoryName.equals(pair.categoryName);
		}

		@Override
		public int compareTo(CategoryQuantPair o) {
			return o.quantity-quantity;
		}	
	}
	
	static class ProductInfo{
		String catName;
		double price;
		
		public ProductInfo(String catName, double price) {
			super();
			this.catName = catName;
			this.price = price;
		}
	}
}
