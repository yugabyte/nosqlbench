package io.nosqlbench.activitytype.ysql;

import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.nosqlbench.engine.api.activityimpl.ParameterMap;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.net.SocketTimeoutException;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class YSQLActivityTest {
    @Test
    public void testErrorNameMapper() {
        ActivityDef activityDef = new ActivityDef(ParameterMap.parseParams("").orElseThrow());
        MYSQLActivity activity = new MYSQLActivity(activityDef);

        // When the Throwable is a SQLException, the error name should be getSQLState()
        Throwable sqlException = new SQLException("my test exception", "my-test-sql-state");
        assertEquals("my-test-sql-state", activity.errorNameMapper(sqlException));

        // See PSQLState to string code mapping at the Github source code website
        // https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/util/PSQLState.java
        Throwable psqlException = new PSQLException("retry transaction", PSQLState.CONNECTION_FAILURE);
        assertEquals("08006", activity.errorNameMapper(psqlException));

        // When Throwable is not a SQLException, the error name should be the class name
        Throwable runtimeException = new SocketTimeoutException("my test runtime exception");
        assertEquals("SocketTimeoutException", activity.errorNameMapper(runtimeException));
    }
}
