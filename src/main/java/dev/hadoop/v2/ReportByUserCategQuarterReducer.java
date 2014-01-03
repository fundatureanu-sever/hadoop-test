package dev.hadoop.v2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v2.intermediate.DataByCatIdAndQuarter;
import dev.hadoop.v2.intermediate.ProductIdQuantityQuarter;
import dev.hadoop.v2.intermediate.UserCategoryId;

public class ReportByUserCategQuarterReducer extends Reducer<UserCategoryId, ProductIdQuantityQuarter, IntWritable, DataByCatIdAndQuarter> {
	
	private HashMap<Integer, Double> productIdToPriceMap = new HashMap<Integer, Double>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		String productsFileName = MetadataProvider.METADATA_FILENAME_BASE+"_products";
		
		BufferedReader productsReader = new BufferedReader(new FileReader(productsFileName));

		String line;
		while ((line = productsReader.readLine()) != null) {
			String[] tokens = line.split("\\t");
			
			int productId = Integer.parseInt(tokens[0]);
			double price = Double.parseDouble(tokens[2]);
			productIdToPriceMap.put(productId, price);
		}

		productsReader.close();
	}

	@Override
	public void reduce(UserCategoryId uidCatId, Iterable<ProductIdQuantityQuarter> values, Context context) throws IOException, InterruptedException {
		int []quantityPerQuarter = new int[4];
		double []revenuePerQuarter = new double[4];
		
		for (ProductIdQuantityQuarter productIdQuantityQuarter : values) {
			
			Double price = productIdToPriceMap.get(productIdQuantityQuarter.getProdId());
			if (price == null){
				System.err.println("No price found for product "+productIdQuantityQuarter.getProdId()+" ; not including it in the total");
				continue;
			}
			
			int quantity = productIdQuantityQuarter.getQuantity();
			byte quarter = productIdQuantityQuarter.getQuarter();
			quantityPerQuarter[quarter] += quantity;
			revenuePerQuarter[quarter] += price*quantity;
		}
		
		for (int i = 0; i < revenuePerQuarter.length; i++) {
			DataByCatIdAndQuarter outValue = new DataByCatIdAndQuarter(uidCatId.getCategoryId(), (byte)i, quantityPerQuarter[i], revenuePerQuarter[i]);
			context.write(new IntWritable(uidCatId.getUserId()), outValue);
		}
	}
	
}
