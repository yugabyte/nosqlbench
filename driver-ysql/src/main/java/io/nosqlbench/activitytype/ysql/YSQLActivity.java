package io.nosqlbench.activitytype.ysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.nosqlbench.activitytype.jdbc.api.JDBCActivity;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.util.Properties;

public class YSQLActivity extends JDBCActivity {
    private static final Logger LOGGER = LogManager.getLogger(YSQLActivity.class);

    public YSQLActivity(ActivityDef activityDef) {
        super(activityDef);
    }

    // TODO provide an error handler with sane defaults including
    //   * retry on 40001 SQL state code
    //   * retry (implement exponential, to avoid stampeding herd) on timeout getting connection from connection pool
    //
    //@Override
    //public NBErrorHandler getErrorHandler() {
    //}

    @Override
    protected DataSource newDataSource() {
        String serverName = getParams().
            getOptionalString("serverName", "localhost").get();

        Integer portNumber = getParams().getOptionalInteger("portNumber").orElse(5433);
        Integer maxPoolSize = getParams().getOptionalInteger("maxPoolSize").orElse(10);
        String databaseName = getParams().getOptionalString("databaseName").orElse("yugabyte");
        String user = getParams().getOptionalString("user").orElse(null);
        String password = getParams().getOptionalString("password").orElse(null);

        Properties poolProperties = new Properties();
        poolProperties.setProperty("dataSourceClassName", "com.yugabyte.ysql.YBClusterAwareDataSource");
        //the pool will create  10 connections to the servers
        poolProperties.setProperty("maximumPoolSize", String.valueOf(maxPoolSize));
        poolProperties.setProperty("dataSource.serverName", serverName);
        poolProperties.setProperty("dataSource.portNumber", String.valueOf(portNumber));
        poolProperties.setProperty("dataSource.databaseName", databaseName);
        if (user != null) {
            poolProperties.setProperty("dataSource.user", "yugabyte");
        }
        if (password != null) {
            poolProperties.setProperty("dataSource.password", "yugabyte");
        }
        // If you want to provide additional end points
        String additionalEndpoints = getParams().getOptionalString("additionalEndpoints").orElse(null);
        if (additionalEndpoints != null) {
            poolProperties.setProperty("dataSource.additionalEndpoints", additionalEndpoints);
        }

        // If you want to load balance between specific geo locations using topology keys
        String topologyKeys = getParams().getOptionalString("topologyKeys").orElse(null);
        if (topologyKeys != null) {
            poolProperties.setProperty("dataSource.topologyKeys", topologyKeys);
        }


        HikariConfig config = new HikariConfig(poolProperties);
        config.validate();
        return new HikariDataSource(config);
    }
}
