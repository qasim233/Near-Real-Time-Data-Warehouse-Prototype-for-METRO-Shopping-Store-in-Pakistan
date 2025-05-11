package com.meshjoin;

import java.sql.*;
import java.io.*;
import java.util.*;

public class MESHJOINDataWarehouse {
    private static final int BUFFER_SIZE = 1000; // Number of records to process in each batch
    private Connection mysqlConnection;
    private BufferedReader transactionReader;
    private List<Map<String, String>> customerPartition;
    private List<Map<String, String>> productPartition;

    public MESHJOINDataWarehouse(String dbUrl, String username, String password, String transactionFilePath) throws Exception {
        // Establish MySQL Connection
        mysqlConnection = DriverManager.getConnection(dbUrl, username, password);
        
        // Open Transaction CSV File
        transactionReader = new BufferedReader(new FileReader(transactionFilePath));
        // Skip header if exists
        transactionReader.readLine();
    }

    private List<Map<String, String>> loadPartition(String tableName) throws SQLException {
        List<Map<String, String>> partition = new ArrayList<>();
        String query = "SELECT * FROM " + tableName;
        try (Statement stmt = mysqlConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                Map<String, String> record = new HashMap<>();
                ResultSetMetaData metaData = rs.getMetaData();
                
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    record.put(metaData.getColumnName(i), rs.getString(i));
                }
                partition.add(record);
            }
        }
        return partition;
    }

    private Map<String, String> performJoin(Map<String, String> transaction, 
                                            List<Map<String, String>> customerPartition, 
                                            List<Map<String, String>> productPartition) {
        Map<String, String> enrichedTransaction = new HashMap<>(transaction);

        // Join with Customer Dimension
        for (Map<String, String> customer : customerPartition) {
            if (customer.get("customer_id").equals(transaction.get("customer_id"))) {
                enrichedTransaction.put("customer_name", customer.get("customer_name"));
                enrichedTransaction.put("gender", customer.get("gender"));
                break;
            }
        }

        // Join with Product Dimension
        for (Map<String, String> product : productPartition) {
            if (product.get("productID").equals(transaction.get("ProductID"))) {
                enrichedTransaction.put("productName", product.get("productName"));
                enrichedTransaction.put("productPrice", product.get("productPrice"));
                enrichedTransaction.put("supplierName", product.get("supplierName"));
                enrichedTransaction.put("storeID", product.get("storeID"));
                enrichedTransaction.put("storeName", product.get("storeName"));
                enrichedTransaction.put("supplierID", product.get("supplierID"));

                // Calculate TotalSales
                double quantity = Double.parseDouble(transaction.get("Quantity Ordered"));
                double price = Double.parseDouble(product.get("productPrice"));
                enrichedTransaction.put("TotalSales", String.valueOf(quantity * price));
                break;
            }
        }

        return enrichedTransaction;
    }

    private void writeToDataWarehouse(List<Map<String, String>> enrichedTransactions) throws SQLException {
        String insertQuery = "INSERT INTO star_schema_transactions " +
            "(Order_ID, Order_Date, ProductID, Quantity_Ordered, customer_id, time_id, " +
            "customer_name, gender, productName, productPrice, supplierName, " +
            "storeID, storeName, supplierID, TotalSales) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = mysqlConnection.prepareStatement(insertQuery)) {
            for (Map<String, String> transaction : enrichedTransactions) {
                pstmt.setString(1, transaction.get("Order ID"));
                pstmt.setString(2, transaction.get("Order Date"));
                pstmt.setString(3, transaction.get("ProductID"));
                pstmt.setString(4, transaction.get("Quantity Ordered"));
                pstmt.setString(5, transaction.get("customer_id"));
                pstmt.setString(6, transaction.get("time_id"));
                pstmt.setString(7, transaction.get("customer_name"));
                pstmt.setString(8, transaction.get("gender"));
                pstmt.setString(9, transaction.get("productName"));
                pstmt.setString(10, transaction.get("productPrice"));
                pstmt.setString(11, transaction.get("supplierName"));
                pstmt.setString(12, transaction.get("storeID"));
                pstmt.setString(13, transaction.get("storeName"));
                pstmt.setString(14, transaction.get("supplierID"));
                pstmt.setString(15, transaction.get("TotalSales"));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    public void performMESHJOIN() throws Exception {
        String line;
        List<Map<String, String>> transactionBatch = new ArrayList<>();

        // Load initial partitions
        customerPartition = loadPartition("customers");
        productPartition = loadPartition("products");

        while ((line = transactionReader.readLine()) != null) {
            String[] fields = line.split(",");
            Map<String, String> transaction = new HashMap<>();
            transaction.put("Order ID", fields[0]);
            transaction.put("Order Date", fields[1]);
            transaction.put("ProductID", fields[2]);
            transaction.put("Quantity Ordered", fields[3]);
            transaction.put("customer_id", fields[4]);
            transaction.put("time_id", fields[5]);

            Map<String, String> enrichedTransaction = performJoin(transaction, customerPartition, productPartition);
            transactionBatch.add(enrichedTransaction);

            if (transactionBatch.size() >= BUFFER_SIZE) {
                writeToDataWarehouse(transactionBatch);
                transactionBatch.clear();
            }
        }

        // Write any remaining transactions
        if (!transactionBatch.isEmpty()) {
            writeToDataWarehouse(transactionBatch);
        }

        // Close resources
        transactionReader.close();
        mysqlConnection.close();
    }

    public static void main(String[] args) {
        try {
            MESHJOINDataWarehouse meshJoin = new MESHJOINDataWarehouse(
                "jdbc:mysql://localhost:3306/MetroDW", 
                "root", 
                "7030", 
                "E:\\Education\\Data Warehouse\\Project\\transactions.csv"
            );
            meshJoin.performMESHJOIN();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
