package io.nosqlbench.activitytype.ycql.statements.rsoperators;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import io.nosqlbench.activitytype.ycql.api.ResultSetCycleOperator;
import io.nosqlbench.activitytype.ycql.errorhandling.exceptions.ResultSetVerificationException;

/**
 * Throws a {@link ResultSetVerificationException} unless there is exactly one row in the result set.
 */
public class AssertSingleRowResultSet implements ResultSetCycleOperator {

    @Override
    public int apply(ResultSet resultSet, Statement statement, long cycle) {
        int rowsIncoming = resultSet.getAvailableWithoutFetching();
        if (rowsIncoming<1) {
            throw new ResultSetVerificationException(cycle, resultSet, statement, "no row in result set, expected exactly 1");
        }
        if (rowsIncoming>1) {
            throw new ResultSetVerificationException(cycle, resultSet, statement, "more than one row in result set, expected exactly 1");
        }
        return rowsIncoming;
    }

}
