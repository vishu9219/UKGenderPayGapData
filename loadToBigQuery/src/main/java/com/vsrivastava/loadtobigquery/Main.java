package com.vsrivastava.loadtobigquery;

import com.vsrivastava.loadtobigquery.data.LoadPartitionedTable;
import com.vsrivastava.loadtobigquery.dependency.DependencyProperties;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties properties = DependencyProperties.getProperties();
        LoadPartitionedTable loadPartitionedTable = new LoadPartitionedTable(properties);
        loadPartitionedTable.runLoadPartitionedTable();
    }
}
