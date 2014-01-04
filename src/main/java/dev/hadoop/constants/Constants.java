package dev.hadoop.constants;

public class Constants {
	
	public static final String TAB = "\t";
	public static final String ESCAPED_TAB = "\\t";
	public static final String EOL = "\n";
	public static final String DASH = "-";
	
	public static final byte N_QUARTERS = 4;
	
	public static final long INPUT_SPLIT_SIZE = 64*1024*1024;
	
	public static final byte Q1 = 0;
	public static final byte Q2 = 1;
	public static final byte Q3 = 2;
	public static final byte Q4 = 3;
	
	//field offsets in log files
	public static final int HEADER_LENGTH = 6;
	
	public static final int DATE_OFFSET = 0;
	public static final int ORDER_ID_OFFSET = 1;
	public static final int PRODUCT_ID_OFFSET = 2;
	public static final int USER_ID_OFFSET = 3;
	public static final int ACTMNGR_ID_OFFSET = 4;
	public static final int QUANTITY_OFFSET = 5;
	
	//metadata column indexes
	public static final int PRODUCT_ID_COL = 1;
	public static final int PRODUCT_NAME_COL = 2;
	public static final int CATEGORY_NAME_COL = 3;
	public static final int PRICE_COL = 4;
	
}
