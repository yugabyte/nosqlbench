package io.nosqlbench.activitytype.ysql;

import io.nosqlbench.engine.api.activityapi.core.Action;
import io.nosqlbench.engine.api.activityapi.core.ActionDispenser;

public class MYSQLActionDispenser implements ActionDispenser {
    private final MYSQLActivity activity;

    public MYSQLActionDispenser(MYSQLActivity a) {
        activity = a;
    }

    @Override
    public Action getAction(int slot) {
        return new MYSQLAction(activity, slot);
    }
}
