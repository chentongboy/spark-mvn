package com.kong.learning.acm;
/**
 * e.g.given [1,2,3,4], return [24,12,8,6].
 * @author kong
 *
 */
public class ExceptSelf {

	public int[] productExceptSelf(int[] nums) {  
        /*int len = nums.length; 
        int [] res = new int[len]; 
         
        if(len < 2) return res; 
         
        int [] left = new int[len]; 
        int [] right = new int[len]; 
         
        left[0] = 1; 
        right[len - 1] = 1; 
        for(int i = len - 1; i > 0; i--){ 
            right[i - 1] = right[i] * nums[i]; 
        } 
         
        for(int i = 0; i < len - 1; i++){ 
            left[i + 1] = left[i] * nums[i]; 
        } 
         
        for(int i = 0; i < len; i++){ 
            res[i] = left[i] * right[i]; 
        } 
        return res;*/  
          
        int len = nums.length;  
        int [] res = new int[len];  
          
        if(len < 2) return res;  
  
        res[len - 1] = 1;  
        for(int i = len - 1; i > 0; i--){  
            res[i - 1] = res[i] * nums[i];  
        }  
          
        int left = 1;  
        for(int i = 0; i < len; i++){  
            res[i] *= left;  
            left = left * nums[i];  
              
        }  
          
        return res;  
    }  
}
