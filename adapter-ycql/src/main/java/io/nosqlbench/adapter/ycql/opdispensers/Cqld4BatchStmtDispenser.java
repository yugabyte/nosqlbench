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
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.nosqlbench.adapter.ycql.RSProcessors;
import io.nosqlbench.adapter.ycql.optypes.Cqld4CqlOp;
import io.nosqlbench.adapter.ycql.optypes.Cqld4CqlBatchStatement;
import io.nosqlbench.engine.api.activityimpl.uniform.DriverAdapter;
import io.nosqlbench.engine.api.templating.ParsedOp;
import io.nosqlbench.api.errors.OpConfigError;
import io.nosqlbench.virtdata.core.templates.ParsedStringTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.LongFunction;

public class Cqld4BatchStmtDispenser extends Cqld4PreparedStmtDispenser {
    private final static Logger logger = LogManager.getLogger(Cqld4BatchStmtDispenser.class);

    private final int batchSize;

    public Cqld4BatchStmtDispenser(
        DriverAdapter adapter, LongFunction<CqlSession> sessionFunc, ParsedOp op, ParsedStringTemplate stmtTpl, RSProcessors processors, int batchSize) {
        super(adapter, sessionFunc, op, stmtTpl, processors);
        this.batchSize = batchSize;
    }

    @Override
    public Cqld4CqlOp apply(long cycle) {

        BatchStatementBuilder bstmtBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
        BoundStatement firstStmt = (BoundStatement) stmtFunc.apply(cycle * batchSize);
        bstmtBuilder.addStatement(firstStmt);
        if (batchSize > 1) {
            List<Integer> pkIndices = firstStmt.getPreparedStatement().getPartitionKeyIndices();                      
            ColumnDefinitions colDefs = firstStmt.getPreparedStatement().getVariableDefinitions();                    
                                                                                                     
            for (int i = 1; i < batchSize; i++) {                                                    
                BoundStatement bStmtNext = (BoundStatement) stmtFunc.apply(cycle * batchSize + i);
                for (int j : pkIndices) {                                         
                    bStmtNext = CQLD4PreparedStmtDiagnostics.bindStatement(                                         
                        bStmtNext, colDefs.get(j).getName(), firstStmt.getObject(j), colDefs.get(j).getType());
                }                                                                                    
                bstmtBuilder.addStatement(bStmtNext);                                                                
            }
        }
        return new Cqld4CqlBatchStatement(
            boundSession,
            bstmtBuilder.build(),
            getMaxPages(),
            getMaxLwtRetries(),
            isRetryReplace()
        );
    }
}
