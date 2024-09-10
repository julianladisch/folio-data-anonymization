package org.folio.anonymizer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class EntryPoint {

  private static Connection getConnection() {
    try (InputStream input = EntryPoint.class.getClassLoader().getResourceAsStream("db.properties")) {
      if (input == null) {
        System.out.println("Unable to find db.properties");
        return null;
      }

      Properties properties = new Properties();
      properties.load(input);

      String url = properties.getProperty("db.url");
      String user = properties.getProperty("db.username");
      String password = properties.getProperty("db.password");

      Class.forName("org.postgresql.Driver");
      return DriverManager.getConnection(url, user, password);
    } catch (ClassNotFoundException | SQLException | IOException e) {
      System.err.println("Connection failed: " + e.getMessage());
      return null;
    }
  }

  public static void main(String[] args) {
    try (
      Connection connection = getConnection();
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT version()")
    ) {
      if (connection != null && rs.next()) {
        System.out.println("PostgreSQL version: " + rs.getString(1));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
