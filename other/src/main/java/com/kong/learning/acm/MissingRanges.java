package com.kong.learning.acm;

import java.util.ArrayList;
import java.util.List;
/**
 *  given [0, 1, 3, 50, 75], lower = 0 and upper = 99, return ["2", "4->49", "51->74", "76->99"].
 * @author kong
 *
 */
public class MissingRanges {

	public List<String> findMissingRanges(int[] nums,int lower,int upper) {
		List<String> res = new ArrayList<String>();
		int pre =lower-1;
		int cur;
		for (int i = 0; i < nums.length; i++) {
			if(i==nums.length)
				cur=upper+1;
			else cur=nums[i];
			if (cur-pre>=2) {
				int missStart = pre+1;
				int missEnd=cur-1;
				if(missEnd == missStart)
					res.add(String.valueOf(missEnd));
				else
					res.add(missStart+"->"+missEnd);
				pre =cur;
			}else {
				pre = cur;
			}
		}
		return res;
	}
}
