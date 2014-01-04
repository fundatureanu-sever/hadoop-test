package dev.hadoop.v2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static dev.hadoop.constants.Constants.ESCAPED_TAB;
import static dev.hadoop.constants.Constants.DATE_OFFSET;
import static dev.hadoop.constants.Constants.PRODUCT_ID_OFFSET;
import static dev.hadoop.constants.Constants.QUANTITY_OFFSET;
import static dev.hadoop.constants.Constants.USER_ID_OFFSET;
import static dev.hadoop.constants.Constants.HEADER_LENGTH;

import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v2.intermediate.ProductIdQuantityQuarter;
import dev.hadoop.v2.intermediate.UserCategoryId;

public class ReportByUserCategQuarterMapper extends Mapper<LongWritable, Text, UserCategoryId, ProductIdQuantityQuarter> {
	static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	private HashMap<Integer, Integer> productToCategoryIdMap = new HashMap<Integer, Integer>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		String productsFileName = "products";
		
		BufferedReader productsReader = new BufferedReader(new FileReader(productsFileName));

		String line;
		while ((line = productsReader.readLine()) != null) {
			String[] tokens = line.split(ESCAPED_TAB+"+");
			
			int productId = Integer.parseInt(tokens[0]);
			int categoryid = Integer.parseInt(tokens[1]);
			productToCategoryIdMap.put(productId, categoryid);
		}

		productsReader.close();
	}


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String []tokens = value.toString().split(ESCAPED_TAB+"+");
		
		if (tokens.length != HEADER_LENGTH){
			System.err.println("Log line at offset: "+key.get()+" has "+tokens.length+" elements instead of "+HEADER_LENGTH);
			return;
		}
		
		try {
			Date date = dateFormat.parse(tokens[DATE_OFFSET]);
			dateFormat.getCalendar().setTime(date);
			int month = dateFormat.getCalendar().get(Calendar.MONTH);
			byte quarter = (byte)(month/3);
			
			int productId = Integer.parseInt(tokens[PRODUCT_ID_OFFSET]);
			int userId = Integer.parseInt(tokens[USER_ID_OFFSET]);
			int quantity = Integer.parseInt(tokens[QUANTITY_OFFSET]);
			
			UserCategoryId outKey = new UserCategoryId(userId, productToCategoryIdMap.get(productId));
			ProductIdQuantityQuarter outValue = new ProductIdQuantityQuarter(productId, quantity, quarter);
			context.write(outKey, outValue);
			
		} catch (ParseException e) {
			System.err.println("Unexpected Date Format on log line at offset: "+key.get()+": "+tokens[DATE_OFFSET]);
			return;
		}
	}

}
