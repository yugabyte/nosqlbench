package io.nosqlbench.activitytype.ysql;

import io.nosqlbench.activitytype.jdbc.api.JDBCActionDispenser;
import io.nosqlbench.engine.api.activityapi.core.ActionDispenser;
import io.nosqlbench.engine.api.activityapi.core.ActivityType;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.nosqlbench.nb.annotations.Service;

@Service(value = ActivityType.class, selector = "ysql")
public class YSQLActivityType implements ActivityType<YSQLActivity> {

    @Override
    public ActionDispenser getActionDispenser(YSQLActivity activity) {
        return new JDBCActionDispenser(activity);
    }

    @Override
    public YSQLActivity getActivity(ActivityDef activityDef) {
        return new YSQLActivity(activityDef);
    }
}
