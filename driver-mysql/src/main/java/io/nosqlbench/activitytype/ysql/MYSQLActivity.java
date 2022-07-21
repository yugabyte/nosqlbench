package io.nosqlbench.activitytype.ysql;

import io.nosqlbench.activitytype.jdbc.api.JDBCActivity;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mysql.cj.jdbc.MysqlDataSource;

import javax.sql.DataSource;

import java.io.IOException;
import java.util.Arrays;

public class MYSQLActivity extends JDBCActivity {
    private static final Logger LOGGER = LogManager.getLogger(MYSQLActivity.class);

    public MYSQLActivity(ActivityDef activityDef) {
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
    	MysqlDataSource mysqlDS = null;
		try {
			mysqlDS = new MysqlDataSource();
			mysqlDS.setURL(getParams().getOptionalString("url").orElse(null));
			mysqlDS.setUser(getParams().getOptionalString("username").orElse(null));
			mysqlDS.setPassword(getParams().getOptionalString("password").orElse(null));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mysqlDS;
    }
}
