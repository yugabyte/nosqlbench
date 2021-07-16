package io.nosqlbench.activitytype.ycql.statements.rsoperators;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import io.nosqlbench.activitytype.ycql.api.ResultSetCycleOperator;

import java.util.LinkedList;

public class RowCapture implements ResultSetCycleOperator {
    @Override
    public int apply(ResultSet resultSet, Statement statement, long cycle) {
        ThreadLocal<LinkedList<Row>> rows = PerThreadCQLData.rows;
        return 0;
    }
}
