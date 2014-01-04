package test.hadoop.v2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Before;
import org.junit.Test;

import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v2.ReportByUserCategQuarterReducer;
import dev.hadoop.v2.intermediate.DataByCatIdAndQuarter;
import dev.hadoop.v2.intermediate.ProductIdQuantityQuarter;
import dev.hadoop.v2.intermediate.UserCategoryId;

import static dev.hadoop.constants.Constants.Q1;
import static dev.hadoop.constants.Constants.Q2;
import static dev.hadoop.constants.Constants.Q3;
import static dev.hadoop.constants.Constants.Q4;

public class ReportByUserCategQuarterReducerTest {
	
	
	
	@Before
	public void prepareTests(){		
		try {
			FileWriter productsWriter = new FileWriter(MetadataProvider.METADATA_FILENAME_BASE+"_products");
			
			productsWriter.write("1	2	1.00 \n");
			productsWriter.write("2	2	0.75 \n");
			productsWriter.write("4	2	1.00 \n");
			productsWriter.write("9998	2	30.00 \n");	
			
			productsWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	@Test
	public void testExpectedInput_Reducer() {
		ReportByUserCategQuarterReducer reducer = new ReportByUserCategQuarterReducer();
		
		try {
			//Given
			int userId = 1;
			int categoryId = 2;
			UserCategoryId uidCatId = new UserCategoryId(userId, categoryId);
			
			ProductIdQuantityQuarter[] values = new ProductIdQuantityQuarter[5];
			values[0] = new ProductIdQuantityQuarter(1, 10, Q1);
			values[1] = new ProductIdQuantityQuarter(2, 10, Q1);
			values[2] = new ProductIdQuantityQuarter(2, 10, Q2);
			values[3] = new ProductIdQuantityQuarter(4, 10, Q2);
			values[4] = new ProductIdQuantityQuarter(9998, 10, Q4);
			
			//When
			Context context = mock(Context.class);
			reducer.setup(context);
			reducer.reduce(uidCatId, Arrays.asList(values), context);
			
			//Then
			DataByCatIdAndQuarter outValue1 = new DataByCatIdAndQuarter(categoryId, Q1, 20, 17.5);
			DataByCatIdAndQuarter outValue2 = new DataByCatIdAndQuarter(categoryId, Q2, 20, 17.5);
			DataByCatIdAndQuarter outValue3 = new DataByCatIdAndQuarter(categoryId, Q3, 0, 0);
			DataByCatIdAndQuarter outValue4 = new DataByCatIdAndQuarter(categoryId, Q4, 10, 300.0);
			
			IntWritable userIdWritable = new IntWritable(userId);
			verify(context).write(userIdWritable, outValue1);
			verify(context).write(userIdWritable, outValue2);
			verify(context).write(userIdWritable, outValue3);
			verify(context).write(userIdWritable, outValue4);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		
	}

}
