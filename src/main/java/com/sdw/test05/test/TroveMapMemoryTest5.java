package com.sdw.test05.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import gnu.trove.TObjectIntHashMap;

public class TroveMapMemoryTest5 {

    public static void main(String[] args) throws InterruptedException {
        System.gc();
        long total = Runtime.getRuntime().totalMemory(); // byte
        long m1 = Runtime.getRuntime().freeMemory();
        long beforeByte = total - m1;
        System.out.println("before:" + beforeByte);
        
        TObjectIntHashMap<String> map = new TObjectIntHashMap<>();
        BufferedReader reader = null;
        
        int byteCount = 0;
        try {
//            String fileName = "map_test01.txt";
            String fileName = "CoreNatureDictionary.ngram.txt";
            Resource resource = new ClassPathResource(fileName);
            InputStream in = resource.getInputStream();
            
            
            reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            String line;
            while((line = reader.readLine()) != null){
                byteCount += line.getBytes().length;
                map.put(line, 1);
            }
            
            in.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally{
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    
                }
            }
        }
        
        System.out.println("original size:" + byteCount/1024 + "KB");
        long total1 = Runtime.getRuntime().totalMemory();
        long m2 = Runtime.getRuntime().freeMemory();
        long afterByte = total1 - m2;
        System.out.println("after:" + afterByte);
        System.out.println("Map size:" + (afterByte - beforeByte)/ 1024 + "KB");
        
        Thread.sleep(500 * 1000);
    }

}
