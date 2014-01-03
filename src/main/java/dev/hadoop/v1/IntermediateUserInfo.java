package dev.hadoop.v1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class IntermediateUserInfo implements Writable{
	static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	int productId;
	int quantity;
	Date date;
	
	public IntermediateUserInfo() {
		super();
	}

	public IntermediateUserInfo(int productId, int quantity, Date date) {
		super();
		this.productId = productId;
		this.quantity = quantity;
		this.date = date;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		productId = in.readInt();
		quantity = in.readInt();
		try {
			date = dateFormat.parse(in.readUTF());
		} catch (ParseException e) {
			throw new IOException(e.getMessage());
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(productId);
		out.writeInt(quantity);
		out.writeUTF(dateFormat.format(date));
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IntermediateUserInfo)){
			return false;
		}
		
		IntermediateUserInfo other = (IntermediateUserInfo)obj;
		return (productId==other.productId && quantity==other.quantity && date.equals(other.date));
	}
	
	
	
}