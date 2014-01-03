package dev.hadoop.v2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v2.intermediate.DataByCatIdAndQuarter;


public class ReportByUserIdReducer extends Reducer<IntWritable, DataByCatIdAndQuarter, NullWritable, Text> {

	private HashMap<Integer, String> categoryMap = new HashMap<Integer, String>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		String categoryFileName = MetadataProvider.METADATA_FILENAME_BASE+"_categories";
		
		BufferedReader categoryReader = new BufferedReader(new FileReader(categoryFileName));

		String line;
		while ((line = categoryReader.readLine()) != null) {
			String[] tokens = line.split("\\t");
			
			int categoryId = Integer.parseInt(tokens[0]);
			String categoryName = tokens[1];
			categoryMap.put(categoryId, categoryName);
		}

		categoryReader.close();
	}


	@Override
	public void reduce(IntWritable userId, Iterable<DataByCatIdAndQuarter> values, Context context) throws IOException, InterruptedException {
		HashMap<Integer, Integer> categoryQuantityMap = new HashMap<Integer, Integer>(categoryMap.size());
		double[] totalRevenuePerQuarter = new double[4];
		
		for (DataByCatIdAndQuarter dataByCatIdAndQuarter : values) {
			updateCategoryQuantityMap(categoryQuantityMap, dataByCatIdAndQuarter);
			
			totalRevenuePerQuarter[dataByCatIdAndQuarter.getQuarter()] += dataByCatIdAndQuarter.getRevenue();		
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
		int categoryId = dataByCatIdAndQuarter.getCategoryId();
		int quantity = dataByCatIdAndQuarter.getQuantity();
		
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
