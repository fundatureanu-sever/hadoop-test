package dev.hadoop.v2.intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataByCatIdAndQuarter implements Writable{
	int categoryId;
	int quantity;
	double revenue;
	byte quarter;//0-3
	
	public DataByCatIdAndQuarter() {
		super();
	}

	public DataByCatIdAndQuarter(int categoryId, byte quarter, int quantity, double revenue) {
		super();
		this.quantity = quantity;
		this.revenue = revenue;
		this.categoryId = categoryId;
		this.quarter = quarter;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		categoryId = in.readInt();
		quantity = in.readInt();
		revenue = in.readDouble();
		quarter = in.readByte();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(categoryId);
		out.writeInt(quantity);
		out.writeDouble(revenue);
		out.writeByte(quarter);
	}

	public int getCategoryId() {
		return categoryId;
	}

	public int getQuantity() {
		return quantity;
	}

	public double getRevenue() {
		return revenue;
	}

	public byte getQuarter() {
		return quarter;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DataByCatIdAndQuarter)){
			return false;
		}
		
		DataByCatIdAndQuarter other = (DataByCatIdAndQuarter)obj;
		return (categoryId==other.categoryId && quantity==other.quantity && revenue==other.revenue && quarter==other.quarter);
	}
	
	
	
}