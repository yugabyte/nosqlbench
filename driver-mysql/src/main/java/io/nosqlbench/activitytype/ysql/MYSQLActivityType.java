package io.nosqlbench.activitytype.ysql;

import io.nosqlbench.activitytype.jdbc.api.JDBCActionDispenser;
import io.nosqlbench.engine.api.activityapi.core.ActionDispenser;
import io.nosqlbench.engine.api.activityapi.core.ActivityType;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.nosqlbench.nb.annotations.Service;

@Service(value = ActivityType.class, selector = "mysql")
public class MYSQLActivityType implements ActivityType<MYSQLActivity> {

    @Override
    public ActionDispenser getActionDispenser(MYSQLActivity activity) {
        return new JDBCActionDispenser(activity);
    }

    @Override
    public MYSQLActivity getActivity(ActivityDef activityDef) {
        return new MYSQLActivity(activityDef);
    }
}
