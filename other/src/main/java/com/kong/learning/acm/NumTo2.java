package com.kong.learning.acm;
/**
 * given input 43261596 (represented in binary as
 *   00000010100101000001111010011100), return 964176192 (represented in binary as
 *   00111001011110000010100101000000).
 * 
 * @author kong
 * 
 */
public class NumTo2 {

	public int reverseBits(int n) {
		// 1145
		int res = n & 1;
		for (int i = 1; i < 32; i++) {
			n = n >> 1;
			res = res << 1;
			res = res | (n & 1);
		}
		return res;
	}
	
	public static void main(String[] args) {
		int num =43261596;
		NumTo2 numTo2 = new NumTo2();
		int result = numTo2.reverseBits(num);
		System.out.println(result);
	}
}
