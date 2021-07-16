package io.nosqlbench.activitytype.ycql.api;

import com.datastax.driver.core.Statement;

public interface StatementFilter {
    boolean matches(Statement statement);
}
