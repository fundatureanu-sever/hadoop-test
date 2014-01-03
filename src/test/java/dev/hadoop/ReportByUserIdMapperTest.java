package dev.hadoop;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

public class ReportByUserIdMapperTest {
	static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	@SuppressWarnings("unchecked")
	@Test
	public void testExpectedInput_ReportByUserIdMapper(){
		ReportByUserIdMapper mapper = new ReportByUserIdMapper();
		
		Context context = mock(Context.class);
		try {
			String inputLine = "2013-01-01	10000		1	1	1	10";
			
			mapper.map(new LongWritable(10), new Text(inputLine), context);
			
			
			Date date = dateFormat.parse("2013-01-01");
			verify(context).write(new IntWritable(1), new IntermediateUserInfo(1, 10, date));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testExpectedInput2Lines_ReportByUserIdMapper(){
		ReportByUserIdMapper mapper = new ReportByUserIdMapper();
		
		Context context = mock(Context.class);
		try {
			String inputLine1 = "2013-01-01	10000	1	1	1	10";
			String inputLine2 = "2013-01-01	10001	1	2	1	45";
			
			mapper.map(new LongWritable(10), new Text(inputLine1), context);
			mapper.map(new LongWritable(10), new Text(inputLine2), context);
			
			
			Date date = dateFormat.parse("2013-01-01");
			verify(context).write(new IntWritable(1), new IntermediateUserInfo(1, 10, date));
			verify(context).write(new IntWritable(2), new IntermediateUserInfo(1, 45, date));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUnexpectedNumberOfElementsInput_ReportByUserIdMapper(){
		ReportByUserIdMapper mapper = new ReportByUserIdMapper();
		
		Context context = mock(Context.class);
		try {
			String inputLine = "2013-01-01	1	1	10";
			
			mapper.map(new LongWritable(10), new Text(inputLine), context);
					
			verifyZeroInteractions(context);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}
	
	@Test
	public void testUnexpectedDateFormatInput_ReportByUserIdMapper(){
		ReportByUserIdMapper mapper = new ReportByUserIdMapper();
		
		Context context = mock(Context.class);
		try {
			String inputLine = "01.01.2013	10000	1	1	1	10";
			
			mapper.map(new LongWritable(10), new Text(inputLine), context);
			
			verifyZeroInteractions(context);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}

}
