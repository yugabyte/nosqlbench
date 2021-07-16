package io.nosqlbench.activitytype.ycql.api;

import com.datastax.driver.core.Row;

/**
 * An operator interface for consuming ResultSets and producing some
 * int that can be used as a status code in activities.
 */
public interface RowCycleOperator {
    int apply(Row row, long cycle);
}
