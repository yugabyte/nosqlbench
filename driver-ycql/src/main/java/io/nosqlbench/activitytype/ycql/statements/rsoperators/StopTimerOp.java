package io.nosqlbench.activitytype.ycql.statements.rsoperators;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import io.nosqlbench.activitytype.ycql.api.ResultSetCycleOperator;

public class StopTimerOp implements ResultSetCycleOperator {

    @Override
    public int apply(ResultSet resultSet, Statement statement, long cycle) {
        return 0;
    }
}
