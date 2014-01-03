package dev.hadoop.v2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dev.hadoop.v2.IntermData.DataByCatIdAndQuarter;

public class ReportByUserIdReducer extends Reducer<IntWritable, IntermData.DataByCatIdAndQuarter, NullWritable, Text> {

	private HashMap<Integer, String> categoryMap = new HashMap<Integer, String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}


	@Override
	protected void reduce(IntWritable userId, Iterable<DataByCatIdAndQuarter> values, Context context) throws IOException, InterruptedException {
		HashMap<Integer, Integer> categoryQuantityMap = new HashMap<Integer, Integer>(categoryMap.size());
		double[] totalRevenuePerQuarter = new double[4];
		
		for (DataByCatIdAndQuarter dataByCatIdAndQuarter : values) {
			updateCategoryQuantityMap(categoryQuantityMap, dataByCatIdAndQuarter);
			
			totalRevenuePerQuarter[dataByCatIdAndQuarter.quarter] += dataByCatIdAndQuarter.revenue;		
		}
		
		String mostPopularCategoryName = findMostPopularCategory(categoryQuantityMap);
		
		String outputLine = buildOutputLine(userId, totalRevenuePerQuarter, mostPopularCategoryName);
		context.write(NullWritable.get(), new Text(outputLine));
	}

	private String buildOutputLine(IntWritable userId, double[] totalRevenuePerQuarter, String mostPopularCategoryName) {
		StringBuilder outputLine = new StringBuilder(userId.get()+"");
		outputLine.append("\t");
		outputLine.append(mostPopularCategoryName);
		for (int i = 0; i < totalRevenuePerQuarter.length; i++) {
			outputLine.append("\t");
			if (totalRevenuePerQuarter[i]==0){
				outputLine.append("-");
			}
			else{
				outputLine.append(totalRevenuePerQuarter[i]);
			}
		}
	
		return outputLine.toString();
	}


	private void updateCategoryQuantityMap(HashMap<Integer, Integer> categoryQuantityMap, DataByCatIdAndQuarter dataByCatIdAndQuarter) {
		int categoryId = dataByCatIdAndQuarter.categoryId;
		int quantity = dataByCatIdAndQuarter.quantity;
		
		Integer categorySum = categoryQuantityMap.get(categoryId);
		if (categorySum==null){
			categoryQuantityMap.put(categoryId, quantity);
		}
		else{
			categoryQuantityMap.put(categoryId, categorySum+quantity);
		}
	}


	private String findMostPopularCategory(HashMap<Integer, Integer> categoryQuantities) {
		int mostPopularCategId=-1, max=0;
		for (Map.Entry<Integer, Integer> entry : categoryQuantities.entrySet()) {
			if (entry.getValue()>max){
				max = entry.getValue();
				mostPopularCategId = entry.getKey();
			}
		}
		
		String mostPopularCategoryName;
		if (mostPopularCategId!=-1){
			mostPopularCategoryName = categoryMap.get(mostPopularCategId);
		}
		else{
			mostPopularCategoryName = "-";
		}
		return mostPopularCategoryName;
	}
	
}
