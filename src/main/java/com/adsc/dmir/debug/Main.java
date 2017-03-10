package com.adsc.dmir.debug;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by kailin on 8/3/17.
 */
public class Main {

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String res;

        String _s;
        try {
            _s = in.nextLine();
        }catch (Exception e){
            _s = null;
        }
        res = search(_s);
        System.out.println(res);
    }

    private static String search(String s) {
        String substr = null;
        HashMap<String,Integer> map = new HashMap<String, Integer>();
        for(int i = 2; i <= s.length()-1; i++) {
            for(int j = 0; j< s.length() && j+i < s.length();j+=i) {
                String temp = s.substring(j, j + i);
                for (int t = 0; t <= map.size(); t++) {
                    if(map.containsKey(temp)){
                        int in =map.get(temp)+1;
                        map.put(temp,in);
                    }else {
                        map.put(temp, 1);
                    }
                }
            }
        }
        int max = 0;

        for(int k =0 ;k<map.size();k++){
            if(max <map.)
        }
        return substr;
    }
}
