package io.nosqlbench.activitytype.ycql.statements.rsoperators;

import com.datastax.driver.core.Row;

import java.util.LinkedList;

/**
 * This contains a linked list of {@link Row} objects. This is per-thread.
 * You can use this list as a per-thread data cache for sharing data between
 * cycles in the same thread.
 */
public class PerThreadCQLData {
    public final static ThreadLocal<LinkedList<Row>> rows = ThreadLocal.withInitial(LinkedList::new);
}
