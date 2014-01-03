package dev.hadoop.v2.intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ProductIdQuantityQuarter implements Writable{
	int prodId;
	int quantity;
	byte quarter;
	
	public ProductIdQuantityQuarter() {
		super();
	}
	
	public ProductIdQuantityQuarter(int prodId, int quantity, byte quarter) {
		super();
		this.prodId = prodId;
		this.quantity = quantity;
		this.quarter = quarter;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		prodId = in.readInt();
		quantity = in.readInt();
		quarter = in.readByte();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(prodId);
		out.writeInt(quantity);
		out.writeByte(quarter);
	}

	public int getProdId() {
		return prodId;
	}

	public int getQuantity() {
		return quantity;
	}

	public byte getQuarter() {
		return quarter;
	}
	
}