package org.humbird.up.hdfs;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by david on 16/9/13.
 */
public class MainTest {
    @Test
    public void testOracle() throws SQLException {
        String driverClass = "oracle.jdbc.OracleDriver";
        try
        {
            Class.forName(driverClass);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(new StringBuilder().append("Could not load db driver class: ").append(driverClass).toString());
        }
        String username = "stargate";
        String password = "stargate";
        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@172.16.50.24:1521:dev", properties);
//        String connectStr = "jdbc:oracle:thin:@172.16.50.24:1521:STARGATE";
        connection.close();
    }
}
