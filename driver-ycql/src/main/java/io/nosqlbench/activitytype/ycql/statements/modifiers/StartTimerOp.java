package io.nosqlbench.activitytype.ycql.statements.modifiers;

import com.datastax.driver.core.Statement;

public class StartTimerOp implements StatementModifier {

    @Override
    public Statement modify(Statement unmodified, long cycleNum) {

        return unmodified;
    }
}
