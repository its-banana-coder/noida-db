// JDBC (MySQL Connector/J) against noida-db, with client-side and
// server-side (useServerPrepStmts=true) prepared statements.
import java.math.BigDecimal;
import java.sql.*;

public class T {
  static int pass = 0, fail = 0;
  static void check(String n, Object got, Object want) {
    if (java.util.Objects.equals(got, want)) pass++;
    else { fail++; System.out.println("  FAIL " + n + "\n    got  " + got + "\n    want " + want); }
  }
  public static void main(String[] a) throws Exception {
    String port = System.getenv().getOrDefault("NOIDA_MYSQL_PORT", "3306");
    for (String mode : new String[]{"false", "true"}) {
      String url = "jdbc:mysql://127.0.0.1:" + port + "/test?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=" + mode;
      try (Connection c = DriverManager.getConnection(url, "root", "")) {
        Statement s = c.createStatement();
        s.execute("DROP TABLE IF EXISTS j");
        s.execute("CREATE TABLE j (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20) NOT NULL, price DECIMAL(6,2), at DATETIME, ok BOOLEAN)");
        PreparedStatement ps = c.prepareStatement("INSERT INTO j (name, price, at, ok) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, "a"); ps.setBigDecimal(2, new BigDecimal("9.99")); ps.setTimestamp(3, Timestamp.valueOf("2024-03-05 14:07:09")); ps.setBoolean(4, true);
        check(mode + " insert count", ps.executeUpdate(), 1);
        ResultSet keys = ps.getGeneratedKeys(); keys.next();
        check(mode + " generated key", keys.getLong(1), 1L);
        PreparedStatement q = c.prepareStatement("SELECT id, name, price, at, ok FROM j WHERE id = ? AND name LIKE ?");
        q.setLong(1, 1); q.setString(2, "a%");
        ResultSet r = q.executeQuery(); r.next();
        check(mode + " row", r.getLong(1) + "|" + r.getString(2) + "|" + r.getBigDecimal(3) + "|" + r.getTimestamp(4) + "|" + r.getBoolean(5),
              "1|a|9.99|2024-03-05 14:07:09.0|true");
        check(mode + " metadata type", r.getMetaData().getColumnTypeName(3), "DECIMAL");
        c.setAutoCommit(false);
        s.execute("DELETE FROM j");
        c.rollback();
        c.setAutoCommit(true);
        ResultSet cnt = s.executeQuery("SELECT COUNT(*) FROM j"); cnt.next();
        check(mode + " rollback", cnt.getInt(1), 1);
        try { s.execute("INSERT INTO j (id, name) VALUES (1, 'dup')"); check(mode + " dup key", "no error", 1062); }
        catch (SQLException e) { check(mode + " dup key", e.getErrorCode(), 1062); }
        DatabaseMetaData md = c.getMetaData();
        ResultSet tables = md.getTables("test", null, "j", new String[]{"TABLE"});
        check(mode + " DatabaseMetaData.getTables", tables.next() ? tables.getString("TABLE_NAME") : null, "j");
        ResultSet cols = md.getColumns("test", null, "j", null);
        int n = 0; while (cols.next()) n++;
        check(mode + " getColumns", n, 5);
      }
    }
    System.out.println("jdbc: " + pass + "/" + (pass + fail) + " checks passed");
    System.exit(fail > 0 ? 1 : 0);
  }
}
