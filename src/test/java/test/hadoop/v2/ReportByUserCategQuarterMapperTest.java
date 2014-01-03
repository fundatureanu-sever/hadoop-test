package test.hadoop.v2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import dev.hadoop.metadata.MetadataProvider;
import dev.hadoop.v2.ReportByUserCategQuarterMapper;
import dev.hadoop.v2.intermediate.ProductIdQuantityQuarter;
import dev.hadoop.v2.intermediate.UserCategoryId;

public class ReportByUserCategQuarterMapperTest {
	
	@Before
	public void prepareTests(){		
		try {
			FileWriter productsWriter = new FileWriter(MetadataProvider.METADATA_FILENAME_BASE+"_products");
			
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
	public void testExpectedInput_Mapper1(){
		ReportByUserCategQuarterMapper mapper = new ReportByUserCategQuarterMapper();
		
		try {
			String inputLine = "2013-01-01	10000		1	1	1	10";
			Context context = mock(Context.class);
			
			mapper.setup(context);
			mapper.map(new LongWritable(10), new Text(inputLine), context);
			
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
	public void testExpectedInput2Lines_Mapper1(){
		ReportByUserCategQuarterMapper mapper = new ReportByUserCategQuarterMapper();
		
		try {
			String inputLine1 = "2013-01-01	10000		1	1	1	10";
			String inputLine2 = "2013-04-01	10000		4	1	1	20";
			Context context = mock(Context.class);
			
			mapper.setup(context);
			mapper.map(new LongWritable(10), new Text(inputLine1), context);
			mapper.map(new LongWritable(10), new Text(inputLine2), context);
			
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

}
