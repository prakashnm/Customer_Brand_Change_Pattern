package com.hadoop.brandswitch;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class FacebookBrandSwitchReducer extends Reducer<FBUser,Text,Text,Text>{
	public void reduce(FBUser key, Iterable<Text> listofvalues,Context context) throws IOException, InterruptedException
	{
		String prev=null;
		for(Text val:listofvalues)
		{
			if(prev==null)
			{
				prev=val.toString();
			}
			else
			{
				context.write(key.getUser(),new Text("("+prev+","+val+")"));
			}
		}
	}

}
