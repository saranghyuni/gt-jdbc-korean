/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.altibase;

import java.io.IOException;
import java.util.Map;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;

@SuppressWarnings("rawtypes")
public class AltibaseNGDataStoreFactory extends JDBCDataStoreFactory {

    // 인코딩 추가?

    /** parameter for database type */
    public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true, "altibase");

    /** parameter for database instance */
    public static final Param DATABASE = new Param("database", String.class, "Database", false,
            "mydb");

    /** parameter for database port */
    public static final Param PORT = new Param("port", Integer.class, "Port", true, 20300);

    /** parameter for database schema */
    public static final Param SCHEMA = new Param("schema", String.class, "Schema", true, "SYS");

    /** parameter for database user */
    public static final Param USER = new Param("user", String.class, "User", true, "sys");

    /** enables using && in bbox queries */
    public static final Param LOOSEBBOX = new Param("Loose bbox", Boolean.class,
            "Perform only primary filter on bbox", false, Boolean.TRUE);

    /** parameter that enables estimated extends instead of exact ones */
    public static final Param ESTIMATED_EXTENTS = new Param("Estimated extends", Boolean.class,
            "Use the spatial index information to quickly get an estimate of the data bounds",
            false, Boolean.FALSE);

    /** Wheter a prepared statements based dialect should be used, or not */
    public static final Param PREPARED_STATEMENTS = new Param("preparedStatements", Boolean.class,
            "Use prepared statements", false, Boolean.TRUE);

    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
        return new AltibaseDialect(dataStore);
    }

    @Override
    protected String getDatabaseID() {
        return DBTYPE.sample.toString();
    }

    @Override
    public String getDisplayName() {
        return "Altibase";
    }

    public String getDescription() {
        return "ALTIBASE(tm) ALTIBASE HDB 5.5.1+ Database";
    }

    @Override
    protected String getDriverClassName() {
        return "Altibase.jdbc.driver.AltibaseDriver";
    }

    @Override
    public boolean canProcess(Map params) {
        if (!super.canProcess(params)) {
            return false; // was not in agreement with getParametersInfo
        }

        return checkDBType(params);
    }

    @Override
    protected boolean checkDBType(Map params) {
        if (super.checkDBType(params)) {
            try {
                Class.forName("org.geotools.data.altibase.AltibaseNGDataStoreFactory");
                return true;
            } catch (ClassNotFoundException e) {
                return true;
            }
        } else {
            return checkDBType(params, "altibase");
        }
    }

    @SuppressWarnings("unchecked")
    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map params)
            throws IOException {
        // database schema
        String schema = (String) USER.lookUp(params);
        if (schema != null) {
            // Altibase 에서 Schema는 테이블의 소유자를 의미한다.
            dataStore.setDatabaseSchema(schema.toUpperCase());
        }

        // setup loose bbox
        AltibaseDialect dialect = (AltibaseDialect) dataStore.getSQLDialect();
        Boolean loose = (Boolean) LOOSEBBOX.lookUp(params);
        dialect.setLooseBBOXEnabled(loose == null || Boolean.TRUE.equals(loose));

        // check the estimated extents
        Boolean estimated = (Boolean) ESTIMATED_EXTENTS.lookUp(params);
        dialect.setEstimatedExtentsEnabled(estimated == null || Boolean.TRUE.equals(estimated));

        // setup the ps dialect if need be
        Boolean usePs = (Boolean) PREPARED_STATEMENTS.lookUp(params);
        if (usePs == null) {
            dataStore.setSQLDialect(new AltibasePSDialect(dataStore, dialect));
        } else {
            if (Boolean.TRUE.equals(usePs)) {
                dataStore.setSQLDialect(new AltibasePSDialect(dataStore, dialect));
            }
        }

        // primary key finder
        dataStore.setPrimaryKeyFinder(new AltibasePrimaryKeyFinder());

        return dataStore;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setupParameters(Map parameters) {
        // NOTE: when adding parameters here remember to add them to AltibaseNGJNDIDataStoreFactory
        super.setupParameters(parameters);
        parameters.put(DBTYPE.key, DBTYPE);
        parameters.put(DATABASE.key, DATABASE);
        parameters.put(PORT.key, PORT);
        parameters.put(USER.key, USER);
        parameters.put(SCHEMA.key, SCHEMA);
        parameters.put(LOOSEBBOX.key, LOOSEBBOX);
        parameters.put(ESTIMATED_EXTENTS.key, ESTIMATED_EXTENTS);
        parameters.put(PREPARED_STATEMENTS.key, PREPARED_STATEMENTS);
        parameters.put(MAX_OPEN_PREPARED_STATEMENTS.key, MAX_OPEN_PREPARED_STATEMENTS);
    }

    @Override
    protected String getValidationQuery() {
        return "SELECT SYSDATE FROM DUAL";
    }

    @Override
    public boolean isAvailable() {
        try {
            Class.forName(getDriverClassName());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected String getJDBCUrl(Map params) throws IOException {
        String host = (String) HOST.lookUp(params);
        String db = (String) DATABASE.lookUp(params);
        Integer port = (Integer) PORT.lookUp(params);

        return "jdbc:Altibase" + "://" + host + ":" + port + "/" + db;
    }

}
