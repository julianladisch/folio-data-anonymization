package org.folio.anonymizer;

import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.jdbi.v3.core.Jdbi;

@Log4j2
@UtilityClass
public class Database {

  private static Jdbi instance = null;

  public static Jdbi getInstance() {
    if (instance != null) {
      return instance;
    }

    // create JDBI from env vars for postgres
    String host = System.getenv("DB_HOST");
    String port = System.getenv("DB_PORT");
    String user = System.getenv("DB_USER");
    String pass = System.getenv("DB_PASS");
    String db = System.getenv("DB_NAME");

    String url = "jdbc:postgresql://" + host + ":" + port + "/" + db;
    log.fatal("Connecting to DB {}", url);

    instance = Jdbi.create(url, user, pass);
    return instance;
  }

  public static String getSchemaName(String module) {
    return System.getenv("TENANT") + "_" + module.replace("-", "_");
  }

  public static String getTableName(String module, String table) {
    return getSchemaName(module) + "." + table;
  }
}
