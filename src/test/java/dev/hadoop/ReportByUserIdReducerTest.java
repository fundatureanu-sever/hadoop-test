package dev.hadoop;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Before;
import org.junit.Test;

import dev.hadoop.metadata.MetadataProvider;

public class ReportByUserIdReducerTest {
	static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	@Before
	public void prepareTests(){
		FileWriter writer;
		try {
			writer = new FileWriter(MetadataProvider.METADATA_FILE_NAME);
			
			writer.write("1	Fruit	1.00\n");
			writer.write("2	Fruit	0.75\n");
			writer.write("4	Decoration	1.00\n");
			writer.write("9998	Car accessories	30.00");	
			
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	@Test
	public void testMostPopularCategory_ReportByUserIdReducer(){
		try {
			ReportByUserIdReducer reducer = new ReportByUserIdReducer();
			
			Context context = mock(Context.class);
			
			IntWritable userId = new IntWritable(1);
			
			Date date = dateFormat.parse("2013-01-01");
			IntermediateUserInfo []userInfo = new IntermediateUserInfo[5];
			userInfo[0] = new IntermediateUserInfo(1, 10, date);
			userInfo[1] = new IntermediateUserInfo(2, 20, date);
			userInfo[2] = new IntermediateUserInfo(4, 10, date);
			userInfo[3] = new IntermediateUserInfo(1, 30, date);
			userInfo[4] = new IntermediateUserInfo(9998, 15, date);
			
			reducer.setup(context);
			reducer.reduce(userId, Arrays.asList(userInfo), context);
			
			//Fruit is the most popular category because it has 60 units sold
			//Revenue only in Q1 = 40*1.00 + 0.75*20 + 10*1.00 +15*30.0 = 515.00 
			String expected = "1	Fruit	515.0	-	-	-";
			verify(context).write(NullWritable.get(), new Text(expected));
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}	
	}
	
	
	@Test
	public void testRevenueDivision_ReportByUserIdReducer(){
		try {
			ReportByUserIdReducer reducer = new ReportByUserIdReducer();
			
			Context context = mock(Context.class);
			
			IntWritable userId = new IntWritable(1);
			
			Date dateQ1_1 = dateFormat.parse("2013-01-01");
			Date dateQ1_2 = dateFormat.parse("2013-03-31");
			Date dateQ2_1 = dateFormat.parse("2013-04-01");
			Date dateQ2_2 = dateFormat.parse("2013-06-30");
			Date dateQ3_1 = dateFormat.parse("2013-07-01");
			Date dateQ3_2 = dateFormat.parse("2013-09-30");
			Date dateQ4_1 = dateFormat.parse("2013-10-01");
			Date dateQ4_2 = dateFormat.parse("2013-12-31");
			IntermediateUserInfo []userInfo = new IntermediateUserInfo[8];
			userInfo[0] = new IntermediateUserInfo(1, 20, dateQ1_1);
			userInfo[1] = new IntermediateUserInfo(1, 20, dateQ1_2);
			userInfo[2] = new IntermediateUserInfo(1, 10, dateQ2_1);
			userInfo[3] = new IntermediateUserInfo(1, 10, dateQ2_2);
			userInfo[4] = new IntermediateUserInfo(1, 30, dateQ3_1);
			userInfo[5] = new IntermediateUserInfo(1, 30, dateQ3_2);
			userInfo[6] = new IntermediateUserInfo(1, 15, dateQ4_1);
			userInfo[7] = new IntermediateUserInfo(1, 15, dateQ4_2);
			
			reducer.setup(context);
			reducer.reduce(userId, Arrays.asList(userInfo), context);
			
			//Fruit is the single category 
			String expected = "1	Fruit	40.0	20.0	60.0	30.0";
			verify(context).write(NullWritable.get(), new Text(expected));
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}	
		
	}
	
	

}
