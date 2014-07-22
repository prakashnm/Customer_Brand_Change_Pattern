package com.hadoop.brandswitch;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FacebookBrandSwitchPartitioner extends Partitioner<FBUser, Text> {
	
	

		@Override
		public int getPartition(FBUser inputMapKey, Text inputMapValue, int numOfReducer) {
			int partitionNum = 0;
			String user=inputMapKey.getUser().toString();
			switch(Integer.parseInt(user)%2) {
				case 0:
					partitionNum = 0;
					break;
				case 1:
					partitionNum = 1;
					break;
				default:  partitionNum = 1;
			}
			return partitionNum;
			
		}

	}


