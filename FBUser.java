package com.hadoop.brandswitch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FBUser implements WritableComparable<FBUser>{
	private Text user;
	private Text timestamp;
	public FBUser()
	{
		this(new Text(),new Text());
	}
	
	public FBUser(String user,String timestamp)
	{
		this(new Text(user),new Text(timestamp));
	}
	public FBUser(Text user,Text timestamp)
	{
		this.user=user;
		this.timestamp=timestamp;
		
	}
	public Text getUser()
	{
		return user;
	}
	public void setUser(Text user)
	{
		this.user=user;
	}
	public Text getTimestamp()
	{
		return timestamp;
	}
	public void setTimestamp(Text timestamp)
	{
		this.timestamp=timestamp;

	}
	
	
	
	public int compareTo(FBUser o)
	{
		int comparison=user.compareTo(o.user);
		if(comparison ==0)
		{
			return timestamp.compareTo(o.timestamp);
		}
		return comparison;
		
	}
	
	public int hashcode()
	{
		return user.hashCode()*31+timestamp.hashCode();
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o instanceof FBUser)
		{
			FBUser fbuser=(FBUser)o;
			return user.equals(fbuser.user)&& timestamp.equals(fbuser.timestamp);
		}
		return false;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		user.readFields(in);
		timestamp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		user.write(out);
		timestamp.write(out);
	}
}
