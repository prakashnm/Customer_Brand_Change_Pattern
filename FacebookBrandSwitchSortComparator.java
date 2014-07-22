package com.hadoop.brandswitch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class FacebookBrandSwitchSortComparator extends WritableComparator{
	private static final Text.Comparator Text_COMPARATOR=new Text.Comparator();
	protected FacebookBrandSwitchSortComparator()
	{
		super(FBUser.class);
	}
	public int compare(byte[] b1,int s1, int l1,byte[] b2,int s2,int l2)
	{
		try
		{
			int firstL1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
			int firstL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
			int cmp=Text_COMPARATOR.compare(b1,s1,firstL1,b2,s2,firstL2);
			if(cmp !=0)
			{
				return cmp;
			}
			return Text_COMPARATOR.compare(b1,s1+firstL1,l1-firstL1,b2,s2+firstL2,l2-firstL2);
		}
		catch(IOException e)
		{
			throw new IllegalArgumentException(e);
		}
		
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		if(a instanceof FBUser && b instanceof FBUser)
		{
			FBUser p1=(FBUser)a;
			FBUser p2=(FBUser)b;
			int comparison=p1.getUser().compareTo(p2.getUser());
			if(comparison ==0)
			{
				return p1.getTimestamp().compareTo(p2.getTimestamp());
			}
			return comparison;
		}
		return super.compare(a,b);
	}

}
