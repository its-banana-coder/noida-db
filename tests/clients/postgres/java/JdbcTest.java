// The PostgreSQL JDBC driver against noida: prepared statements, batches,
// transactions, and the DatabaseMetaData calls Hibernate and Flyway make.

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class JdbcTest {
    static int checks = 0;

    static void check(String what, Object got, Object want) {
        checks++;
        String g = String.valueOf(got);
        String w = String.valueOf(want);
        if (!g.equals(w)) {
            throw new AssertionError(what + ": got " + g + ", want " + w);
        }
    }

    public static void main(String[] args) throws Exception {
        String port = System.getenv().getOrDefault("PGPORT", "5432");
        String url = "jdbc:postgresql://127.0.0.1:" + port + "/postgres";
        try (Connection c = DriverManager.getConnection(url, "postgres", "postgres")) {
            check("autocommit", c.getAutoCommit(), true);
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS jdbc_items");
                s.execute("CREATE TABLE jdbc_items ("
                        + "id serial PRIMARY KEY,"
                        + "name varchar(50) NOT NULL,"
                        + "qty int,"
                        + "price numeric(8,2),"
                        + "ok boolean,"
                        + "when_ timestamp,"
                        + "day date,"
                        + "tags text[])");
            }

            // Prepared statement with typed parameters and generated keys.
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO jdbc_items (name, qty, price, ok, when_, day) VALUES (?,?,?,?,?,?) RETURNING id")) {
                ps.setString(1, "widget");
                ps.setInt(2, 3);
                ps.setBigDecimal(3, new BigDecimal("9.99"));
                ps.setBoolean(4, true);
                ps.setTimestamp(5, Timestamp.valueOf("2020-05-06 07:08:09"));
                ps.setDate(6, Date.valueOf("2020-05-06"));
                try (ResultSet rs = ps.executeQuery()) {
                    check("returning", rs.next(), true);
                    check("generated id", rs.getInt(1), 1);
                }
            }

            // Reading values back with the right JDBC types.
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM jdbc_items WHERE name = ?")) {
                ps.setString(1, "widget");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    check("varchar", rs.getString("name"), "widget");
                    check("int", rs.getInt("qty"), 3);
                    check("numeric", rs.getBigDecimal("price"), new BigDecimal("9.99"));
                    check("boolean", rs.getBoolean("ok"), true);
                    check("timestamp", rs.getTimestamp("when_"), Timestamp.valueOf("2020-05-06 07:08:09"));
                    check("date", rs.getDate("day"), Date.valueOf("2020-05-06"));
                    check("null array", rs.getArray("tags"), null);

                    ResultSetMetaData md = rs.getMetaData();
                    check("column count", md.getColumnCount(), 8);
                    check("column label", md.getColumnLabel(1), "id");
                    check("column type", md.getColumnType(3), Types.INTEGER);
                    check("varchar type", md.getColumnType(2), Types.VARCHAR);
                    check("varchar size", md.getPrecision(2), 50);
                    check("numeric scale", md.getScale(4), 2);
                    check("nullable", md.isNullable(2), ResultSetMetaData.columnNoNulls);
                }
            }

            // An array parameter.
            try (PreparedStatement ps = c.prepareStatement("UPDATE jdbc_items SET tags = ? WHERE id = ?")) {
                Array tags = c.createArrayOf("text", new String[] {"a", "b"});
                ps.setArray(1, tags);
                ps.setInt(2, 1);
                check("array update", ps.executeUpdate(), 1);
            }
            try (Statement s = c.createStatement();
                    ResultSet rs = s.executeQuery("SELECT tags FROM jdbc_items WHERE id = 1")) {
                rs.next();
                String[] tags = (String[]) rs.getArray(1).getArray();
                check("array value", String.join(",", tags), "a,b");
            }

            // Batches, as ORMs flush them.
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO jdbc_items (name, qty) VALUES (?,?)")) {
                for (int i = 2; i <= 5; i++) {
                    ps.setString(1, "item" + i);
                    ps.setInt(2, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                check("batch size", counts.length, 4);
                int total = 0;
                for (int n : counts) {
                    total += n;
                }
                check("batch rows", total, 4);
            }

            // Transactions and rollback.
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.executeUpdate("INSERT INTO jdbc_items (name, qty) VALUES ('rolled back', 0)");
            }
            c.rollback();
            c.setAutoCommit(true);
            try (Statement s = c.createStatement();
                    ResultSet rs = s.executeQuery("SELECT count(*) FROM jdbc_items")) {
                rs.next();
                check("after rollback", rs.getInt(1), 5);
            }

            // SQLSTATE on failure, then the connection keeps working.
            try (Statement s = c.createStatement()) {
                s.executeUpdate("INSERT INTO jdbc_items (id, name) VALUES (1, 'dup')");
                throw new AssertionError("expected a unique violation");
            } catch (SQLException e) {
                check("unique sqlstate", e.getSQLState(), "23505");
            }
            try (Statement s = c.createStatement();
                    ResultSet rs = s.executeQuery("SELECT 1")) {
                rs.next();
                check("usable after error", rs.getInt(1), 1);
            }

            // DatabaseMetaData: what Hibernate, Flyway and Liquibase read.
            java.sql.DatabaseMetaData md = c.getMetaData();
            check("product", md.getDatabaseProductName(), "PostgreSQL");
            check("major version", md.getDatabaseMajorVersion(), 16);
            try (ResultSet rs = md.getTables(null, "public", "jdbc_items", new String[] {"TABLE"})) {
                check("getTables", rs.next(), true);
                check("table name", rs.getString("TABLE_NAME"), "jdbc_items");
            }
            List<String> cols = new ArrayList<>();
            try (ResultSet rs = md.getColumns(null, "public", "jdbc_items", null)) {
                while (rs.next()) {
                    cols.add(rs.getString("COLUMN_NAME"));
                }
            }
            check("getColumns", String.join(",", cols), "id,name,qty,price,ok,when_,day,tags");
            try (ResultSet rs = md.getPrimaryKeys(null, "public", "jdbc_items")) {
                check("getPrimaryKeys", rs.next(), true);
                check("pk column", rs.getString("COLUMN_NAME"), "id");
            }
            try (ResultSet rs = md.getIndexInfo(null, "public", "jdbc_items", false, false)) {
                check("getIndexInfo", rs.next(), true);
            }
            try (ResultSet rs = md.getTypeInfo()) {
                check("getTypeInfo", rs.next(), true);
            }

            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE jdbc_items");
            }
        }
        System.out.println("jdbc: " + checks + " checks passed");
    }
}
