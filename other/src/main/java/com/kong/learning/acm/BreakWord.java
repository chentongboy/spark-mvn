package com.kong.learning.acm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 把字符串切分成句子
 * @author kong
 *
 */
public class BreakWord {

	/**
	 * 分词
	 * @param s
	 * @param dict
	 * @return
	 */
	public List<String> wordBreak(String s, Set<String> dict) {  
        ArrayList<String> res = new ArrayList<String>();  
        if(s == null || s.isEmpty() || !wordBreakCanDo(s, dict)){  
            return res;  
        }  
        dfs(s, dict, 0, "", res);  
        return res;  
    }  
      
    /**
     * 递归扫描  
     * @param s
     * @param dict
     * @param startIndex
     * @param preWords
     * @param res
     */
    public void dfs(String s, Set<String> dict, int startIndex, String preWords, ArrayList<String> res){  
        if(startIndex >= s.length()){  
            //return contition is that the startIndex has been out of bound  
            res.add(preWords);  
            return;  
        }  
        for(int i = startIndex; i < s.length(); i++){  
            String curStr = s.substring(startIndex, i+1);  
            if(dict.contains(curStr)){  
                String newSol;  
                if(preWords.length() > 0){  
                    newSol = preWords + " " + curStr;  
                } else {  
                    newSol = curStr;  
                }  
                dfs(s, dict, i + 1, newSol, res);  
            }  
        }  
    }  
    
    /**
     * 判断是否可分
     * @param s
     * @param dict
     * @return
     */
    public boolean wordBreakCanDo(String s, Set<String> dict) {  
        s = "#" + s;  
        boolean[] canSegmented = new boolean[s.length()];  
          
        canSegmented[0] = true;  
        for(int i = 1; i < s.length(); i++){  
            for(int k = 0; k < i; k++){  
                canSegmented[i] = canSegmented[k] && dict.contains(s.substring(k + 1, i + 1));  
                if(canSegmented[i]) break;  
            }  
        }  
        return canSegmented[s.length() - 1];  
    }  
    
    public static void main(String[] args) {
    	String s = "如果不能直接返回空容器"; 
    	Set<String> dict = new HashSet<String>();
    	dict.add("如果");
    	dict.add("不");
    	dict.add("能");
    	dict.add("不能");
    	dict.add("直接");
    	dict.add("返回");
    	dict.add("空");
    	dict.add("容器");
    	BreakWord breakWord = new BreakWord();
    	List<String> wordBreak = breakWord.wordBreak(s, dict);
    	System.out.println(wordBreak);
	}
}
