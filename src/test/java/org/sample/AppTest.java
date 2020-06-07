package org.sample;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.protocol.Resultset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class AppTest {

    private Connection connection;

    @Before
    public void setupConn() throws SQLException {
        // jdbc:mysql://localhost:3306/database?characterEncoding=utf8
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName("127.0.0.1");
        dataSource.setPort(3306);
        dataSource.setDatabaseName("spring_data");
        dataSource.setUser("root");
        dataSource.setPassword("123456");
        dataSource.setCharacterEncoding("UTF-8");
        dataSource.setProfileSQL(true);
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
    }

    @Test
    public void checkMetaData() throws SQLException{
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        System.out.println(databaseMetaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        System.out.println(databaseMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        System.out.println(databaseMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));

        System.out.println(databaseMetaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));

        System.out.println(databaseMetaData.getResultSetHoldability());
        System.out.println(databaseMetaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test
    public void query() throws SQLException {
        String sql = "select * from user";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            System.out.print(resultSet.getObject("id") + " | ");
            System.out.print(resultSet.getObject("name") + "\r\n");
        }
    }

    @Test
    public void update() throws SQLException {
        String sql = "update user set name=? where id=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0 ; i < 10; i++) {
            ps.setString(1, "batchUpdate" + i);
            ps.setLong(2, i);
            ps.execute();
        }
    }

    @Test
    public void batchUpdate() throws SQLException {
        String sql = "update user set name=? where id=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0 ; i < 10; i++) {
            ps.setString(1, "batchUpdate" + i);
            ps.setLong(2, i);
            ps.addBatch();
        }
        ps.executeBatch();
    }

    @Test
    public void batchDelete() throws SQLException {
        String sql = "delete from user where id=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0 ; i < 10; i++) {
            ps.setLong(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
    }

    @Test
    public void batchInsert() throws SQLException {
        String sql = "insert into user(name) values (?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0 ; i < 10; i++) {
            ps.setString(1, "batchInsert" + i);
            ps.addBatch();
        }
        ps.executeBatch();
    }

    @Test
    public void batchInBatch() throws SQLException {
        final int batchSize = 10;

        String sql = "update user set name=? where id=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0 ; i < 100; i++) {
            ps.setString(1, "batchUpdate" + i);
            ps.setLong(2, i);
            ps.addBatch();
            if (i % batchSize == 0) {
                ps.executeBatch();
            }
        }
        ps.executeBatch();
    }

    @Test
    public void rsTypeAndConcur() throws SQLException {
        String sql = "select * from user";
        PreparedStatement ps = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = ps.executeQuery();
        resultSet.absolute(2);
        resultSet.updateString("name", "English");
        resultSet.updateRow();
        System.out.print(resultSet.getObject("id") + " | ");
        System.out.println(resultSet.getObject("name"));
    }

    @Test
    public void fetchSize() throws SQLException {
        String sql = "select * from user";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setFetchSize(2);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            System.out.print(resultSet.getObject("id") + " | ");
            System.out.println(resultSet.getObject("name"));
        }
    }

    @Test
    public void streamRead() throws SQLException{
        String sql = "select * from user";
        PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet resultSet = stmt.executeQuery();
        while (resultSet.next()) {
            System.out.print(resultSet.getObject("id") + " | ");
            System.out.println(resultSet.getObject("name"));
        }
    }

    @After
    public void cleanupConn() throws SQLException {
        connection.close();
    }
}
