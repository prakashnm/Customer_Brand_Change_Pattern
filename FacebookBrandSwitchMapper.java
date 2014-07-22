package com.hadoop.brandswitch;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FacebookBrandSwitchMapper extends Mapper<LongWritable,Text,FBUser,Text> {
private FBUser fbuser=new FBUser();
public void map(LongWritable inputKey, Text inputVal, Context context) throws InterruptedException, IOException
{
	String s=inputVal.toString();
	StringTokenizer splitstring=new StringTokenizer(s,",");
	String[] splits = new String[3];
	int i=0;
	while(splitstring.hasMoreTokens())
	{
		splits[i]=splitstring.nextToken();
		i++;
	}
//String[] splits= inputVal.toString().split(",");
System.out.println("split 0 is"+splits[0]+" split of 1 is "+splits[1]+" split of 2 is "+splits[2]);
fbuser.setUser(new Text(splits[0]));
fbuser.setTimestamp (new Text(splits[2]));
context.write(fbuser,new Text(splits[1]));
}
	
}
