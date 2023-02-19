/*
 * Copyright (c) 2022 nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.adapter.ycql.opdispensers;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.nosqlbench.adapter.ycql.optypes.Cqld4CqlSimpleStatement;
import io.nosqlbench.engine.api.activityimpl.uniform.DriverAdapter;
import io.nosqlbench.engine.api.templating.ParsedOp;

import java.util.function.LongFunction;

public class Cqld4SimpleCqlStmtDispenser extends Cqld4BaseOpDispenser {

    private final LongFunction<Statement> stmtFunc;
    private final LongFunction<String> targetFunction;

    public Cqld4SimpleCqlStmtDispenser(DriverAdapter adapter, LongFunction<CqlSession> sessionFunc, LongFunction<String> targetFunction, ParsedOp cmd) {
        super(adapter, sessionFunc,cmd);
        this.targetFunction=targetFunction;
        this.stmtFunc =createStmtFunc(cmd);
    }

    protected LongFunction<Statement> createStmtFunc(ParsedOp op) {
        return super.getEnhancedStmtFunc(l -> SimpleStatement.newInstance(targetFunction.apply(l)),op);
    }

    @Override
    public Cqld4CqlSimpleStatement apply(long value) {
        return new Cqld4CqlSimpleStatement(
            getSessionFunc().apply(value),
            (SimpleStatement) stmtFunc.apply(value),
            getMaxPages(),
            isRetryReplace(),
            getMaxLwtRetries()
        );
    }

}
