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
	public void testExpectedInput_Reducer1() {
		ReportByUserCategQuarterReducer reducer = new ReportByUserCategQuarterReducer();
		
		try {
			int userId = 1;
			UserCategoryId uidCatId = new UserCategoryId(userId, 2);
			
			ProductIdQuantityQuarter[] values = new ProductIdQuantityQuarter[5];
			values[0] = new ProductIdQuantityQuarter(1, 10, (byte)0);
			values[1] = new ProductIdQuantityQuarter(2, 10, (byte)0);
			values[2] = new ProductIdQuantityQuarter(2, 10, (byte)1);
			values[3] = new ProductIdQuantityQuarter(4, 10, (byte)1);
			values[4] = new ProductIdQuantityQuarter(9998, 10, (byte)3);
			
			Context context = mock(Context.class);
			reducer.setup(context);
			reducer.reduce(uidCatId, Arrays.asList(values), context);
			
			DataByCatIdAndQuarter outValue1 = new DataByCatIdAndQuarter(2, (byte)0, 20, 17.5);
			DataByCatIdAndQuarter outValue2 = new DataByCatIdAndQuarter(2, (byte)1, 20, 17.5);
			DataByCatIdAndQuarter outValue3 = new DataByCatIdAndQuarter(2, (byte)2, 0, 0);
			DataByCatIdAndQuarter outValue4 = new DataByCatIdAndQuarter(2, (byte)3, 10, 300.0);
			
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
