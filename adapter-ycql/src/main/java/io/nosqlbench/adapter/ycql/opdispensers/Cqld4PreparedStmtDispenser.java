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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.nosqlbench.adapter.ycql.RSProcessors;
import io.nosqlbench.adapter.ycql.optypes.Cqld4CqlOp;
import io.nosqlbench.adapter.ycql.optypes.Cqld4CqlPreparedStatement;
import io.nosqlbench.engine.api.activityimpl.uniform.DriverAdapter;
import io.nosqlbench.engine.api.templating.ParsedOp;
import io.nosqlbench.api.errors.OpConfigError;
import io.nosqlbench.virtdata.core.templates.ParsedStringTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.LongFunction;

public class Cqld4PreparedStmtDispenser extends Cqld4BaseOpDispenser {
    private final static Logger logger = LogManager.getLogger(Cqld4PreparedStmtDispenser.class);

    protected final RSProcessors processors;
    protected final LongFunction<Statement> stmtFunc;
    private final ParsedStringTemplate stmtTpl;
    private final LongFunction<Object[]> fieldsF;
    private PreparedStatement preparedStmt;
    protected CqlSession boundSession;

    public Cqld4PreparedStmtDispenser(
        DriverAdapter adapter, LongFunction<CqlSession> sessionFunc, ParsedOp op, ParsedStringTemplate stmtTpl, RSProcessors processors) {
        super(adapter, sessionFunc, op);
        if (op.isDynamic("space")) {
            throw new RuntimeException("Prepared statements and dynamic space values are not supported." +
                " This would churn the prepared statement cache, defeating the purpose of prepared statements.");
        }
        this.processors = processors;
        this.stmtTpl = stmtTpl;
        this.fieldsF = getFieldsFunction(op);
        stmtFunc = createStmtFunc(fieldsF, op);
    }

    private LongFunction<Object[]> getFieldsFunction(ParsedOp op) {
        LongFunction<Object[]> varbinder;
        varbinder = op.newArrayBinderFromBindPoints(stmtTpl.getBindPoints());
        return varbinder;
    }

    protected LongFunction<Statement> createStmtFunc(LongFunction<Object[]> fieldsF, ParsedOp op) {

        String preparedQueryString = stmtTpl.getPositionalStatement(s -> "?");
        boundSession = getSessionFunc().apply(0);
        try {
            preparedStmt = boundSession.prepare(preparedQueryString);
        } catch (Exception e) {
            throw new OpConfigError(e + "( for statement '" + stmtTpl + "')");
        }

        LongFunction<Statement> boundStmtFunc = c -> {
            Object[] apply = fieldsF.apply(c);
            return preparedStmt.bind(apply);
        };
        return super.getEnhancedStmtFunc(boundStmtFunc, op);
    }

    @Override
    public Cqld4CqlOp apply(long cycle) {

        BoundStatement boundStatement;
        try {
            boundStatement = (BoundStatement) stmtFunc.apply(cycle);
            return new Cqld4CqlPreparedStatement(
                boundSession,
                boundStatement,
                getMaxPages(),
                isRetryReplace(),
                getMaxLwtRetries(),
                processors
            );
        } catch (Exception exception) {
            return CQLD4PreparedStmtDiagnostics.rebindWithDiagnostics(
                preparedStmt,
                fieldsF,
                cycle,
                exception
            );
        }
    }
}
