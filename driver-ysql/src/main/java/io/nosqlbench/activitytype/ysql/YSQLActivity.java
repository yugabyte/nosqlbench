package io.nosqlbench.activitytype.ysql;

import io.nosqlbench.activitytype.jdbc.api.JDBCActivity;
import io.nosqlbench.engine.api.activityapi.core.ActivityDefObserver;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import com.yugabyte.ysql.YBClusterAwareDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Properties;

public class YSQLActivity extends JDBCActivity implements ActivityDefObserver {
	private static final Logger LOGGER = LogManager.getLogger(YSQLActivity.class);

	public YSQLActivity(ActivityDef activityDef) {

		super(activityDef);
	}

	// TODO provide an error handler with sane defaults including
	// * retry on 40001 SQL state code
	// * retry (implement exponential, to avoid stampeding herd) on timeout getting
	// connection from connection pool
	//
	// @Override
	// public NBErrorHandler getErrorHandler() {
	// }

	@Override
	protected DataSource newDataSource() {

		Boolean useHikari = getParams().getOptionalBoolean("hikari").orElse(false);
		/*
		 * YBClusterAwareDataSource ds = new YBClusterAwareDataSource();
		 * 
		 * ds.setServerName(getParams().getOptionalString("serverName",
		 * "localhost").get());
		 * ds.setDatabaseName(getParams().getOptionalString("databaseName").orElse(null)
		 * ); ds.setUser(getParams().getOptionalString("user").orElse(null));
		 * ds.setPassword(getParams().getOptionalString("password").orElse(null));
		 */

		if (useHikari) {
			Properties poolProperties = new Properties();
			poolProperties.setProperty("dataSourceClassName", "com.yugabyte.ysql.YBClusterAwareDataSource");
			poolProperties.setProperty("maximumPoolSize", "10");
			poolProperties.setProperty("dataSource.serverName",
					getParams().getOptionalString("serverName", "localhost").get());
			poolProperties.setProperty("dataSource.portNumber",
					getParams().getOptionalInteger("portNumber").orElse(5433).toString());
			poolProperties.setProperty("dataSource.databaseName",
					getParams().getOptionalString("databaseName").orElse(null));
			poolProperties.setProperty("dataSource.user", getParams().getOptionalString("user").orElse(null));
			poolProperties.setProperty("dataSource.password", getParams().getOptionalString("password").orElse(null));
			poolProperties.setProperty("dataSource.currentSchema", getParams().getOptionalString("schema").orElse("yugabyte"));

			// If you want to provide additional end points
			// String additionalEndpoints =
			// "127.0.0.2:5433,127.0.0.3:5433,127.0.0.4:5433,127.0.0.5:5433";
			// poolProperties.setProperty("dataSource.additionalEndpoints",
			// additionalEndpoints);
			// If you want to load balance between specific geo locations using topology
			// keys
			// String geoLocations = "cloud1.region1.zone1,cloud1.region2.zone2";
			// poolProperties.setProperty("dataSource.topologyKeys", geoLocations);

			poolProperties.setProperty("poolName", "default");

			HikariConfig config = new HikariConfig(poolProperties);
			config.validate();
			HikariDataSource ds = new HikariDataSource(config);
			return ds;
		} else {

			PGSimpleDataSource ds = new PGSimpleDataSource();

			// serverName is required
			String serverName = getParams().getOptionalString("serverName", "localhost").get();

			// portNumber, databaseName, user, password are optional
			Integer portNumber = getParams().getOptionalInteger("portNumber").orElse(5433);
			String databaseName = getParams().getOptionalString("databaseName").orElse(null);
			String user = getParams().getOptionalString("user").orElse(null);
			String password = getParams().getOptionalString("password").orElse(null);

			ds.setServerNames(new String[] { serverName });
			ds.setPortNumbers(new int[] { portNumber });
			if (databaseName != null) {
				ds.setDatabaseName(databaseName);
			}
			if (user != null) {
				ds.setUser(user);
			}
			if (password != null) {
				ds.setPassword(password);
			}
			ds.setCurrentSchema(getParams().getOptionalString("schema").orElse("yugabyte"));

			LOGGER.debug("Final DataSource fields:" + " serverNames=" + Arrays.toString(ds.getServerNames())
					+ " portNumbers=" + Arrays.toString(ds.getPortNumbers()) + " databaseName=" + ds.getDatabaseName()
					+ " user=" + ds.getUser() + " password=" + ds.getPassword());

			return ds;
		}

	}

}
