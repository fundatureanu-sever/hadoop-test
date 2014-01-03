package dev.hadoop.v2;

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

public class ReportByUserCategQuarterMapper extends Mapper<LongWritable, Text, IntermData.UIDCatId, IntermData.ProductIdQuantityQuarter> {
	static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static final int HEADER_LENGTH = 6;
	private HashMap<Integer, Integer> productToCategoryIdMap = new HashMap<Integer, Integer>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String []tokens = value.toString().split("\\t+");
		
		if (tokens.length != HEADER_LENGTH){
			System.err.println("Log line at offset: "+key.get()+" has "+tokens.length+" elements instead of "+HEADER_LENGTH);
			return;
		}
		
		try {
			Date date = dateFormat.parse(tokens[0]);
			dateFormat.getCalendar().setTime(date);
			int month = dateFormat.getCalendar().get(Calendar.MONTH);
			byte quarter = (byte)(month/3);
			
			int productId = Integer.parseInt(tokens[2]);
			int userId = Integer.parseInt(tokens[3]);
			int quantity = Integer.parseInt(tokens[5]);
			
			IntermData.UIDCatId outKey = new IntermData.UIDCatId(userId, productToCategoryIdMap.get(productId));
			IntermData.ProductIdQuantityQuarter outValue = new IntermData.ProductIdQuantityQuarter(productId, quantity, quarter);
			context.write(outKey, outValue);
			
		} catch (ParseException e) {
			System.err.println("Unexpected Date Format on log line at offset: "+key.get()+": "+tokens[0]);
			return;
		}
	}

}
