package test.hadoop.v2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Before;
import org.junit.Test;

import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v2.ReportByUserIdReducer;
import dev.hadoop.v2.intermediate.DataByCatIdAndQuarter;

import static dev.hadoop.constants.Constants.Q1;
import static dev.hadoop.constants.Constants.Q2;
import static dev.hadoop.constants.Constants.Q3;
import static dev.hadoop.constants.Constants.Q4;

public class ReportByUserIdReducerTest {

	@Before
	public void prepareTests(){		
		try {
			FileWriter categoryWriter = new FileWriter(MetadataProvider.METADATA_FILENAME_BASE+"_categories");
			
			categoryWriter.write("0	Fruit\n");
			categoryWriter.write("1	Decoration\n");
			categoryWriter.write("2	Car accessories\n");	
			
			categoryWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	@Test
	public void testExpectedInput_Reducer(){
		try {
			//Given
			
			IntWritable userId = new IntWritable(1);
			
			DataByCatIdAndQuarter []userInfo = new DataByCatIdAndQuarter[12];
			
			int categoryId = 0;
			userInfo[0] = new DataByCatIdAndQuarter(categoryId, Q1, 10, 50.0);
			userInfo[1] = new DataByCatIdAndQuarter(categoryId, Q2, 20, 30.0);
			userInfo[2] = new DataByCatIdAndQuarter(categoryId, Q3, 20, 30.0);
			userInfo[3] = new DataByCatIdAndQuarter(categoryId, Q4, 20, 30.0);
			
			categoryId = 1;
			userInfo[4] = new DataByCatIdAndQuarter(categoryId, Q1, 20, 20.0);
			userInfo[5] = new DataByCatIdAndQuarter(categoryId, Q2, 20, 20.0);
			userInfo[6] = new DataByCatIdAndQuarter(categoryId, Q3, 20, 10.0);
			userInfo[7] = new DataByCatIdAndQuarter(categoryId, Q4, 20, 20.0);
			
			categoryId = 2;
			userInfo[8] = new DataByCatIdAndQuarter(categoryId, Q1, 30, 10.0);
			userInfo[9] = new DataByCatIdAndQuarter(categoryId, Q2, 30, 10.0);
			userInfo[10] = new DataByCatIdAndQuarter(categoryId, Q3, 30, 70.0);
			userInfo[11] = new DataByCatIdAndQuarter(categoryId, Q4, 30, 10.0);
			
			//When
			ReportByUserIdReducer reducer = new ReportByUserIdReducer();
			Context context = mock(Context.class);
			reducer.setup(context);
			reducer.reduce(userId, Arrays.asList(userInfo), context);
			
			//Then
			String expected = "1	Car accessories	80.0	60.0	110.0	60.0";
			verify(context).write(NullWritable.get(), new Text(expected));
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}
}
