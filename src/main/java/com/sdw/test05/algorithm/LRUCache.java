package com.sdw.test05.algorithm;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<K, V> {

    private int cacheSize;
    private Entry<K, V> header;
    private Entry<K, V> last;
    private Map<K, Entry<K, V>> cache;

    public LRUCache(int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("cacheSize must be more than zero.");
        }
        this.cacheSize = cacheSize;
        this.cache = new HashMap<K, Entry<K, V>>(cacheSize);
    }

    public void put(K key, V value) {
        Entry<K, V> entry = cache.get(key);
        if (entry == null) {
            if(this.cache.size() >= cacheSize){
                this.cache.remove(this.last.key);
                this.removeToLast();
            }
            entry = new Entry<K, V>(key, value);
        } else {
            entry.value = value;
        }
        this.cache.put(key, entry);
        this.moveToHeader(entry);
    }
    
    public V get(K key){
        Entry<K, V> entry = this.cache.get(key);
        if(entry == null){
            return null;
        }else{
            this.moveToHeader(entry);
            return entry.value;
        }
    }

    public void moveToHeader(Entry<K, V> entry) {
        if(this.header == null || this.last == null){
            this.header = entry;
            this.last = entry;
            
            return;
        }
        if(this.header == entry){
            return;
        }
        
        if(entry.prev != null){
            entry.prev.next = entry.next;
        }
        
        if(entry.next != null){
            entry.next.prev = entry.prev;
        }
        
        if(entry == this.last){
            this.last = this.last.prev;
        }
        
        Entry<K, V> next = this.header;
        
        this.header = entry;
        this.header.next = next;
        this.header.prev = null;
        
        next.prev = entry;
    }

    private void removeToLast() {
        if(this.last == null){
            return;
        }
        Entry<K, V> prev = this.last.prev;
        this.last = prev;
        if (this.last != null) {
            this.last.next = null;
        }else{
            this.header = null;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Entry<K, V> entry = header;
        int count = 1;
        while (entry != null) {
            if(count > 1){
                sb.append("-->");
            }
            sb.append(String.format("%s:%s ", entry.key, entry.value));
            entry = entry.next;
            count++;
        }
        return sb.toString();
    }

    static class Entry<K, V> {
        Entry<K, V> prev;
        Entry<K, V> next;
        K key;
        V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
    
    public static void main(String[] args) {
        LRUCache<String, String> lru = new LRUCache<>(5);
        
        lru.put("234", "45");
        lru.put("234", "458");
        lru.put("2346", "458");
        
        lru.put("2346", "458");
        
        lru.put("78", "gtt");
        lru.put("7800", "gtt");
        
        lru.put("5600", "gtt");
        System.out.println(lru.get("234"));
        lru.put("5601", "gtt");
        System.out.println(lru);
    }
}
