package dev.hadoop.v1;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ReportByUserIdMapper extends Mapper<LongWritable, Text, IntWritable, IntermediateUserInfo>
{
	private static final int HEADER_LENGTH = 6;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String []tokens = value.toString().split("\\t+");
		
		if (tokens.length != HEADER_LENGTH){
			System.err.println("Log line at offset: "+key.get()+" has "+tokens.length+" elements instead of "+HEADER_LENGTH);
			return;
		}
		
		try {
			Date date = IntermediateUserInfo.dateFormat.parse(tokens[0]);

			int productId = Integer.parseInt(tokens[2]);
			int userId = Integer.parseInt(tokens[3]);
			int quantity = Integer.parseInt(tokens[5]);

			IntermediateUserInfo userInfo = new IntermediateUserInfo(productId, quantity, date);
			
			context.write(new IntWritable(userId), userInfo);
		} catch (ParseException e) {
			System.err.println("Unexpected Date Format on log line at offset: "+key.get()+": "+tokens[0]);
			return;
		}
	}
}