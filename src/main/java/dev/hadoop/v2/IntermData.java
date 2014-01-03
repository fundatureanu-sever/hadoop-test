package dev.hadoop.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntermData {
	
	public static class UIDCatId implements WritableComparable<UIDCatId>{
		int userId;
		int categoryId;

		public UIDCatId() {
			super();
		}

		public UIDCatId(int userId, int categoryId) {
			super();
			this.userId = userId;
			this.categoryId = categoryId;
		}



		@Override
		public void readFields(DataInput in) throws IOException {
			userId = in.readInt();
			categoryId = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(userId);
			out.writeInt(categoryId);
		}

		@Override
		public int compareTo(UIDCatId o) {
			int c1 = userId-o.userId;
			if (c1!=0){
				return c1;
			}
			
			return (categoryId-o.categoryId);
		}

		@Override
		public int hashCode() {
			return userId*31+categoryId;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof UIDCatId)){
				return false;
			}
			
			UIDCatId other = (UIDCatId)obj;
			return (userId==other.userId && categoryId==other.userId);
		}
		
	}
	
	public static class ProductIdQuantityQuarter implements Writable{
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
	}
	
	public static class DataByCatIdAndQuarter implements Writable{
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
		
	}
	

}
