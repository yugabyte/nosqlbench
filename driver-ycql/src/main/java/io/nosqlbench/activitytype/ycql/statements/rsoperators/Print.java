package io.nosqlbench.activitytype.ycql.statements.rsoperators;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import io.nosqlbench.activitytype.ycql.api.ResultSetCycleOperator;

public class Print implements ResultSetCycleOperator {

    @Override
    public int apply(ResultSet resultSet, Statement statement, long cycle) {
        System.out.println("RS:"+ resultSet.toString());
        return 0;
    }
}
