package com.sdw.test05;

import java.util.HashMap;
import java.util.Map;

public class Test01 {

    public static void main(String[] args) {
        Map<Integer, Integer> data = new HashMap<>();
        
        int[] a = {18899005,2,3,4,2,4,18899005,5,3,3};
        for(int val : a){
            if(data.containsKey(val)){
                int tmp = data.get(val);
                data.put(val, ++tmp);
            }else{
                data.put(val, 1);
            }
        }

        System.out.println(data);
    }

}
