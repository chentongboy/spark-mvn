package com.kong.learning.acm;

import java.util.HashSet;

public class SolutionZZ {

	public int lengthOfLongestSubstring(String s) {
		if (s.isEmpty())
			return 0;
		if (s.length() == 1)
			return 1;

		char[] sArray = s.toCharArray();
		int p1 = 0;
		int p2 = 1;
		HashSet<Character> bufferSubString = new HashSet<Character>();
		bufferSubString.add(sArray[p1]);
		int curMax = 0;
		while (p2 < sArray.length) {
			if (bufferSubString.contains(sArray[p2])) {
				if (p2 - p1 > curMax)
					curMax = p2 - p1;
				// move p1
				while (sArray[p1] != sArray[p2]) {
					bufferSubString.remove(sArray[p1]);
					p1++;
				}
				bufferSubString.remove(sArray[p1]);
				p1++;
				bufferSubString.add(sArray[p1]);
				bufferSubString.add(sArray[p2]);
				p2++;
			} else {
				bufferSubString.add(sArray[p2]);
				p2++;
			}
		}
		if (p2 - p1 > curMax)
			curMax = p2 - p1;
		return curMax;
	}

	public static void main(String[] args) {
		String str = "abcabddcbbjjncuu,uc,ytut,sighh,idi.fi.ufml;jjprlnxm.,.ncxoijrpohjs[hjrjhsqanaekj[dgnlnbzkhjoshglgfjhpkjgxhlkgjhshlfgxmhlkjgpfhgpgjlkhjnklgxjh;jgkohjoithribnfgjh;lkdzxjfhogjhlfdzjhlkfjdzgkljoirhnkbnikg,nbzlkdhnjhgpihgb";
		SolutionZZ sZz = new SolutionZZ();
		int end = sZz.lengthOfLongestSubstring(str);
		System.out.println(end);
		System.out.println(str.substring(0, end));

	}
}
