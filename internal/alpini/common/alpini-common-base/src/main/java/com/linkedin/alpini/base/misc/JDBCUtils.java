package com.linkedin.alpini.base.misc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.WillClose;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility methods for JDBC.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public enum JDBCUtils {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private static final Logger LOG = LogManager.getLogger(JDBCUtils.class);

  public static void closeQuietly(@WillClose ResultSet rs) {
    try (ResultSet closeable = rs) {
      LOG.debug("closeQuietly({})", closeable);
    } catch (SQLException ex) {
      LOG.warn("closeQuietly Error", ex);
    }
  }

  public static void closeQuietly(@WillClose Statement stmt) {
    try (Statement closeable = stmt) {
      LOG.debug("closeQuietly({})", closeable);
    } catch (SQLException ex) {
      LOG.warn("closeQuietly Error for stmt={}", stmt, ex);
    }
  }

  public static void closeQuietly(@WillClose Connection conn) {
    try (Connection closeable = conn) {
      LOG.debug("closeQuietly({})", closeable);
    } catch (SQLException ex) {
      LOG.warn("closeQuietly", ex);
    }
  }
}
