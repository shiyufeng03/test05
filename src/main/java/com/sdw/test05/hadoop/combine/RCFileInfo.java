package com.sdw.test05.hadoop.combine;

public class RCFileInfo {
    private int columns;
    
    public RCFileInfo(int columns){
        this.columns = columns;
    }

    public int getColumns() {
        return columns;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + columns;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RCFileInfo other = (RCFileInfo) obj;
        if (columns != other.columns)
            return false;
        return true;
    }
    
    
}
