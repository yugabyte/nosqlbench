package io.nosqlbench.activitytype.ycql.statements.rowoperators;

import com.datastax.driver.core.Row;
import io.nosqlbench.activitytype.ycql.api.RowCycleOperator;

/**
 * Save specific variables to the thread local object map
 */
public class Print implements RowCycleOperator {

    @Override
    public int apply(Row row, long cycle) {
        System.out.println("ROW:" + row);
        return 0;
    }

}
