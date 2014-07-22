package com.hadoop.brandswitch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class FacebookBrandSwitchGroupingComparator extends WritableComparator{
private static final Text.Comparator TEXT_COMPARATOR= new Text.Comparator();
protected FacebookBrandSwitchGroupingComparator()
{
	super(Text.class);	
}

public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2)
{
	try
	{
		int firstL1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
		int firstL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
		return TEXT_COMPARATOR.compare(b1, s1,firstL1,b2,s2,firstL2);
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
	if (a instanceof FBUser && b instanceof FBUser)
		{return ((FBUser) a).getUser().compareTo(((FBUser)b ).getUser());}
	return super.compare(a, b);


}
}
