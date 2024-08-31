package com.example;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Scanner;
import java.sql.Driver;
import java.util.Enumeration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvToDatabase {
    public static void main(String[] args) {

        Dotenv dotenv = Dotenv.load();
        String jdbcUrl = dotenv.get("DB_URL"); // Replace with your database name
        String username = dotenv.get("DB_USERNAME"); // Replace with your MySQL username
        String password = dotenv.get("DB_PASSWORD"); // Replace with your MySQL password
        String csvFilePath = "medals_total.csv"; // Path to your CSV file
        String tableName = "country_medals"; // Replace with your table name

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            System.out.println("Connected to MySQL database successfully!");

            // Drop the table if it exists
            String dropTableSQL = "DROP TABLE IF EXISTS " + tableName;
            connection.createStatement().execute(dropTableSQL);

            // Create a new table
            String createTableSQL = "CREATE TABLE " + tableName + " ("
                    + "country VARCHAR(255) PRIMARY KEY, "
                    + "Gold INT, "
                    + "Silver INT, "
                    + "Bronze INT, "
                    + "Total INT)";
            connection.createStatement().execute(createTableSQL);
            System.out.println("Table " + tableName + " created successfully!");

            // Prepare the insert statement
            String insertSQL = "INSERT INTO " + tableName + " (country, Gold, Silver, Bronze, Total) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
                 BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
                 CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

                for (CSVRecord csvRecord : csvParser) {
                    String countryName = csvRecord.get("country");

                    int gold = Integer.parseInt(csvRecord.get("Gold Medal"));
                    int silver = Integer.parseInt(csvRecord.get("Silver Medal"));
                    int bronze = Integer.parseInt(csvRecord.get("Bronze Medal"));
                    int total = Integer.parseInt(csvRecord.get("Total"));

                    preparedStatement.setString(1, countryName);
                    preparedStatement.setInt(2, gold);
                    preparedStatement.setInt(3, silver);
                    preparedStatement.setInt(4, bronze);
                    preparedStatement.setInt(5, total);
                    preparedStatement.addBatch();
                }

                preparedStatement.executeBatch();
                System.out.println("CSV data inserted into MySQL table successfully!");

            } catch (IOException e) {
                System.out.println("Error reading CSV file.");
                e.printStackTrace();
            }

            // Prompt user for query input
            Scanner scanner = new Scanner(System.in);
            while(true){
                System.out.println("Do you want to run a query on the table? (y/n): ");
                String userInput = scanner.nextLine();

                if ("y".equalsIgnoreCase(userInput)) {
                    System.out.println("Please enter your SQL query:");
                    String query = scanner.nextLine();

                    try (Statement statement = connection.createStatement()) {
                        boolean hasResultSet = statement.execute(query);

                        if (hasResultSet) {
                            try (ResultSet resultSet = statement.getResultSet()) {
                                int columnCount = resultSet.getMetaData().getColumnCount();
                                while (resultSet.next()) {
                                    for (int i = 1; i <= columnCount; i++) {
                                        System.out.print(resultSet.getString(i) + "\t");
                                    }
                                    System.out.println();
                                }
                            }
                        } else {
                            int updateCount = statement.getUpdateCount();
                            System.out.println("Query executed successfully, " + updateCount + " row(s) affected.");
                        }
                    } catch (SQLException e) {
                        System.out.println("Error executing query: " + e.getMessage());
                    }
                } else if ("n".equalsIgnoreCase(userInput)) {
                    System.out.println("Program terminated.");
                    break; // Exit the loop and terminate the program
                } else {
                    System.out.println("Invalid input. Please enter 'y' or 'n'.");
                }
            }

        } catch (SQLException e) {
            System.out.println("Failed to connect to MySQL database.");
            e.printStackTrace();
        } finally {
            // Deregister JDBC driver to prevent warnings
            try {
                Enumeration<Driver> drivers = DriverManager.getDrivers();
                while (drivers.hasMoreElements()) {
                    Driver driver = drivers.nextElement();
                    DriverManager.deregisterDriver(driver);
                }
                System.out.println("JDBC drivers deregistered successfully.");
            } catch (SQLException e) {
                System.out.println("Error deregistering JDBC drivers: " + e.getMessage());
            }
        }

    }
}
