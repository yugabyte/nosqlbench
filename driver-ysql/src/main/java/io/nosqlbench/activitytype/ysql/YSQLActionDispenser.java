package io.nosqlbench.activitytype.ysql;

import io.nosqlbench.engine.api.activityapi.core.Action;
import io.nosqlbench.engine.api.activityapi.core.ActionDispenser;

public class YSQLActionDispenser implements ActionDispenser {
    private final YSQLActivity activity;

    public YSQLActionDispenser(YSQLActivity a) {
        activity = a;
    }

    @Override
    public Action getAction(int slot) {
        return new YSQLAction(activity, slot);
    }
}
