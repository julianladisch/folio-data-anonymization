package org.folio.anonymizer;

public class Database {
    public static final String SCHEMA_NAME = "${tenant}_mod_users";
    public static final String TABLE_NAME = "users";

    public static String getSchemaName() {
        return SCHEMA_NAME;
    }

    public static String getTableName() {
        return TABLE_NAME;
    }
}
