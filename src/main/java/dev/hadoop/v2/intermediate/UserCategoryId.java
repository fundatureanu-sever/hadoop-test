package dev.hadoop.v2.intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserCategoryId implements WritableComparable<UserCategoryId>{
	int userId;
	int categoryId;

	public UserCategoryId() {
		super();
	}

	public UserCategoryId(int userId, int categoryId) {
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
	public int compareTo(UserCategoryId o) {
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
		if (!(obj instanceof UserCategoryId)){
			return false;
		}
		
		UserCategoryId other = (UserCategoryId)obj;
		return (userId==other.userId && categoryId==other.userId);
	}

	public int getUserId() {
		return userId;
	}

	public int getCategoryId() {
		return categoryId;
	}

}