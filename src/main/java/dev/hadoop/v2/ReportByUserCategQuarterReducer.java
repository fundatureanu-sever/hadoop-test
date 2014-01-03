package dev.hadoop.v2;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dev.hadoop.v2.IntermData.ProductIdQuantityQuarter;
import dev.hadoop.v2.IntermData.UIDCatId;

public class ReportByUserCategQuarterReducer extends Reducer< IntermData.UIDCatId, IntermData.ProductIdQuantityQuarter, IntWritable, IntermData.DataByCatIdAndQuarter> {
	
	private HashMap<Integer, Double> productIdToPriceMap = new HashMap<Integer, Double>();
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}


	@Override
	protected void reduce(UIDCatId uidCatId, Iterable<ProductIdQuantityQuarter> values, Context context) throws IOException, InterruptedException {
		int []quantityPerQuarter = new int[4];
		double []revenuePerQuarter = new double[4];
		
		for (ProductIdQuantityQuarter productIdQuantityQuarter : values) {
			
			Double price = productIdToPriceMap.get(productIdQuantityQuarter.prodId);
			if (price == null){
				System.err.println("No price found for product "+productIdQuantityQuarter.prodId+" ; not including it in the total");
				continue;
			}
			
			quantityPerQuarter[productIdQuantityQuarter.quarter] += productIdQuantityQuarter.quantity;
			revenuePerQuarter[productIdQuantityQuarter.quarter] += price*productIdQuantityQuarter.quantity;
		}
		
		for (int i = 0; i < revenuePerQuarter.length; i++) {
			IntermData.DataByCatIdAndQuarter outValue = new IntermData.DataByCatIdAndQuarter(uidCatId.categoryId, (byte)i, quantityPerQuarter[i], revenuePerQuarter[i]);
			context.write(new IntWritable(uidCatId.userId), outValue);
		}
	}
	
}
