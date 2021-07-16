package io.nosqlbench.activitytype.ycql.statements.rsoperators;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import io.nosqlbench.activitytype.ycql.api.ResultSetCycleOperator;
import io.nosqlbench.virtdata.library.basics.core.threadstate.SharedState;

public class ClearVars implements ResultSetCycleOperator {

    @Override
    public int apply(ResultSet resultSet, Statement statement, long cycle) {
        SharedState.tl_ObjectMap.get().clear();
        return 0;
    }
}
