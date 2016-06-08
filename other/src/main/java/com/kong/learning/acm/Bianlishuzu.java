package com.kong.learning.acm;
/**
 * 字符串A=ABCDE，B=ACE，返回1，B=AEC，返回0
 * 字符串A=rabbbit，B=rabbit，返回3
 * @author kong
 *
 */
public class Bianlishuzu {

	public int numDistinct(String S, String T) {  
        //varations of edit distance problems  
        //you can only delete chars to change S to T  
        //if(S.isEmpty()) return 0;  
        //if(T.isEmpty()) return 1;  
        int m = S.length();  
        int n = T.length();  
        int [][] dp = new int[m + 1][n + 1];  
        dp[0][0] = 1;  
        for(int i = 1; i <= m; i++){  
            dp[i][0] = 1;  
        }  
        for(int i = 1; i <= n; i++){  
            dp[0][i] = 0;  
        }  
        for(int i = 1; i <= m; i++){  
            for(int j = 1; j <= n; j++){  
                dp[i][j] = dp[i-1][j] + (S.charAt(i-1) == T.charAt(j-1) ? dp[i-1][j-1] : 0);  
            }  
        }  
        return dp[m][n];  
    }  
	
	public static void main(String[] args) {
		String S = "rabbbit";
		String T = "rabbit";
		Bianlishuzu shuzu = new Bianlishuzu();
		int num = shuzu.numDistinct(S, T);
		System.out.println(num);
	}
}
