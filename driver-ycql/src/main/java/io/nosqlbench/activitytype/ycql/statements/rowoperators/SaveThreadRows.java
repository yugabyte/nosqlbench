package io.nosqlbench.activitytype.ycql.statements.rowoperators;

import com.datastax.driver.core.Row;
import io.nosqlbench.activitytype.ycql.api.RowCycleOperator;
import io.nosqlbench.activitytype.ycql.statements.rsoperators.PerThreadCQLData;

import java.util.LinkedList;

/**
 * Adds the current row to the per-thread row cache.
 */
public class SaveThreadRows implements RowCycleOperator {

    @Override
    public int apply(Row row, long cycle) {
        LinkedList<Row>rows = PerThreadCQLData.rows.get();
        rows.add(row);
        return 0;
    }

}
