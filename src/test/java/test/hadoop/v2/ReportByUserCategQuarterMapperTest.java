package test.hadoop.v2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v1.ReportByUserIdMapper;
import dev.hadoop.v2.ReportByUserCategQuarterMapper;
import dev.hadoop.v2.intermediate.ProductIdQuantityQuarter;
import dev.hadoop.v2.intermediate.UserCategoryId;

public class ReportByUserCategQuarterMapperTest {
	
	@Before
	public void prepareTests(){		
		try {
			FileWriter productsWriter = new FileWriter("products");
			
			productsWriter.write("1	1	1.00\n");
			productsWriter.write("2	1	0.75\n");
			productsWriter.write("4	2	1.00\n");
			productsWriter.write("9998	3	30.00");	
			
			productsWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	@Test
	public void testExpectedInput_Mapper(){
		try {
			//Given
			String inputLine = "2013-01-01	10000		1	1	1	10";
			
			//When
			Context context = mock(Context.class);
			ReportByUserCategQuarterMapper mapper = new ReportByUserCategQuarterMapper();
			mapper.setup(context);
			mapper.map(new LongWritable(10), new Text(inputLine), context);
			
			//Then
			UserCategoryId outKey = new UserCategoryId(1, 1);
			ProductIdQuantityQuarter outValue = new ProductIdQuantityQuarter(1, 10, (byte)0);
			verify(context).write(outKey, outValue);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testExpectedInput2Lines_Mapper(){
		try {
			//Given
			String inputLine1 = "2013-01-01	10000		1	1	1	10";
			String inputLine2 = "2013-04-01	10000		4	1	1	20";
			
			//When
			Context context = mock(Context.class);
			ReportByUserCategQuarterMapper mapper = new ReportByUserCategQuarterMapper();
			mapper.setup(context);
			mapper.map(new LongWritable(10), new Text(inputLine1), context);
			mapper.map(new LongWritable(10), new Text(inputLine2), context);
			
			//Then
			UserCategoryId outKey1 = new UserCategoryId(1, 1);
			ProductIdQuantityQuarter outValue1 = new ProductIdQuantityQuarter(1, 10, (byte)0);
			verify(context).write(outKey1, outValue1);
			
			UserCategoryId outKey2 = new UserCategoryId(1, 2);
			ProductIdQuantityQuarter outValue2 = new ProductIdQuantityQuarter(4, 20, (byte)1);
			verify(context).write(outKey2, outValue2);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUnexpectedNumberOfElementsInput_Mapper(){
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
	public void testUnexpectedDateFormatInput_Mapper(){
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
