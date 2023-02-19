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

package io.nosqlbench.adapter.ycql.optypes;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.nosqlbench.adapter.ycql.*;
import io.nosqlbench.adapter.ycql.exceptions.ChangeUnappliedCycleException;
import io.nosqlbench.adapter.ycql.exceptions.ExceededRetryReplaceException;
import io.nosqlbench.adapter.ycql.exceptions.UndefinedResultSetException;
import io.nosqlbench.adapter.ycql.exceptions.UnexpectedPagingException;
import io.nosqlbench.engine.api.activityimpl.uniform.flowtypes.*;

import java.util.Iterator;
import java.util.Map;


// TODO: add statement filtering
// TODO: add statement post processing for trace capture
// TODO: add trace capture
// TODO: add/document max tries exhausted exception
// TODO: add/document UnexpectedPagingException
// TODO: add/document chnge unapplied exception
// TODO: add total rows metrics
// TODO: add rows histogram resultSetSizeHisto


public abstract class Cqld4CqlOp implements CycleOp<ResultSet>, VariableCapture, OpGenerator, OpResultSize {

    private final CqlSession session;
    private final int maxPages;
    private final boolean retryReplace;
    private final int maxLwtRetries;
    private int retryReplaceCount =0;

    private ResultSet rs;
    private Cqld4CqlOp nextOp;
    private final RSProcessors processors;

    public Cqld4CqlOp(CqlSession session, int maxPages, boolean retryReplace, int maxLwtRetries, RSProcessors processors) {
        this.session = session;
        this.maxPages = maxPages;
        this.retryReplace = retryReplace;
        this.maxLwtRetries =maxLwtRetries;
        this.processors = processors;
    }

    protected Cqld4CqlOp(CqlSession session, int maxPages, boolean retryReplace, int maxLwtRetries, int retryRplaceCount, RSProcessors processors) {
        this.session = session;
        this.maxPages = maxPages;
        this.retryReplace = retryReplace;
        this.maxLwtRetries =maxLwtRetries;
        this.retryReplaceCount=retryRplaceCount;
        this.processors = processors;
    }

    public final ResultSet apply(long cycle) {

        Statement<?> stmt = getStmt();
        rs = session.execute(stmt);
        processors.start(cycle, rs);
        int totalRows = 0;

        if (!rs.wasApplied()) {
            if (!retryReplace) {
                throw new ChangeUnappliedCycleException(rs, getQueryString());
            } else {
                retryReplaceCount++;
                if (retryReplaceCount >maxLwtRetries) {
                    throw new ExceededRetryReplaceException(rs,getQueryString(), retryReplaceCount);
                }
                Row one = rs.one();
                processors.buffer(one);
                totalRows++;
                nextOp = this.rebindLwt(stmt, one);
            }
        }

        // Paginated Op

        Iterator<Row> reader = rs.iterator();
        int pages = 0;
        while (true) {
            int pageRows = rs.getAvailableWithoutFetching();
            for (int i = 0; i < pageRows; i++) {
                Row row = reader.next();
                processors.buffer(row);
            }
            if (pages++ > maxPages) {
                throw new UnexpectedPagingException(rs, getQueryString(), pages, maxPages, stmt.getPageSize());
            }
            if (rs.isFullyFetched()) {
                break;
            }
            totalRows += pageRows;
        }
        processors.flush();
        return rs;
    }

    @Override
    public Op getNextOp() {
        Op next = nextOp;
        nextOp = null;
        return next;
    }

    @Override
    public Map<String, ?> capture() {
        if (rs == null) {
            throw new UndefinedResultSetException(this);
        }
        return null;
    }

    public abstract Statement<?> getStmt();

    public abstract String getQueryString();

    private Cqld4CqlOp rebindLwt(Statement<?> stmt, Row row) {
        BoundStatement rebound = LWTRebinder.rebindUnappliedStatement(stmt, row);
        return new Cqld4CqlReboundStatement(session, maxPages, retryReplace, maxLwtRetries, retryReplaceCount, rebound, processors);
    }

}
