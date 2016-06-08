package com.kong.learning.acm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 * 去掉重复字符
 * @author kong
 *
 */
public class Solution {

	public static void main(String[] args) {
		String str = "abcabddcbbjjncuu,uc,ytut,sighh,idi.fi.ufml;jjprlnxm.,.ncxoijrpohjs[hjrjhsqanaekj[dgnlnbzkhjoshglgfjhpkjgxhlkgjhshlfgxmhlkjgpfhgpgjlkhjnklgxjh;jgkohjoithribnfgjh;lkdzxjfhogjhlfdzjhlkfjdzgkljoirhnkbnikg,nbzlkdhnjhgpihgb";
		String str11 = "";
		//System.out.println(str.charAt(1));
		int length=0;
		Map<Integer, String> map = new HashMap<Integer, String>();
		Set<String> set = new HashSet<String>();
		for (int i = 0; i < str.length(); i++) {
			String str1 = str.substring(i, i+1);
			set.add(str1);
			if (set.size()>length) {
				map.put(set.size()-1, str1);
				length++;
			}
			
		}
		
		for (int i = 0; i < map.size(); i++) {
			str11=str11.concat(map.get(i));
		}
		System.out.println(str11);
//		System.out.println(set);
		
	}
}
