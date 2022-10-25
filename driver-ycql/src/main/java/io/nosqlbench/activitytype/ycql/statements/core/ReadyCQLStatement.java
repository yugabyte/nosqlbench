package io.nosqlbench.activitytype.ycql.statements.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import io.nosqlbench.activitytype.ycql.api.ResultSetCycleOperator;
import io.nosqlbench.activitytype.ycql.api.RowCycleOperator;
import io.nosqlbench.activitytype.ycql.core.CQLBindHelper;
import io.nosqlbench.engine.api.metrics.ThreadLocalNamedTimers;
import io.nosqlbench.virtdata.core.bindings.ContextualArrayBindings;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A ReadyCQLStatement instantiates new statements to be executed at some mix ratio.
 * It optionally holds metrics objects for a named statement.
 */
public class ReadyCQLStatement {

    private final String name;
    private final ContextualArrayBindings<?, Statement> contextualBindings;
    private long ratio;
    private ResultSetCycleOperator[] resultSetOperators = null;
    private RowCycleOperator[] rowCycleOperators = null;

    private Timer successTimer;
    private Timer errorTimer;
    private Histogram rowsFetchedHisto;
    private Writer resultCsvWriter;
    private List<String> startTimers;
    private List<String> stopTimers;
    int batchSize;

    public ReadyCQLStatement(ContextualArrayBindings<?, Statement> contextualBindings, long ratio, String name, int batchSize) {
        this.contextualBindings = contextualBindings;
        this.ratio = ratio;
        this.name = name;
        this.batchSize = batchSize;
    }

    public ReadyCQLStatement withMetrics(Timer successTimer, Timer errorTimer, Histogram rowsFetchedHisto) {
        this.successTimer = successTimer;
        this.errorTimer = errorTimer;
        this.rowsFetchedHisto = rowsFetchedHisto;
        return this;
    }

    public Statement bind(long cycle) {
        if (batchSize > 1) {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            // Get all the BoundStatements and rebind partition key values of all subsequent
            // statements with the first one
            BoundStatement bStmt = (BoundStatement) contextualBindings.bind(cycle);
            batch.add(bStmt);

            int[] pkIndices = bStmt.preparedStatement().getRoutingKeyIndexes();
            ColumnDefinitions colDefs = bStmt.preparedStatement().getVariables();

            for (int i = 1; i < batchSize; i++) {
                BoundStatement bStmtNext = (BoundStatement) contextualBindings.bind(cycle * batchSize + i);
                for (int j = 0; j < pkIndices.length; j++) {
                    bStmtNext = CQLBindHelper.bindStatement(
                        bStmtNext, colDefs.getName(pkIndices[j]), bStmt.getObject(pkIndices[j]), colDefs.getType(pkIndices[j]).getName());
                }
                batch.add(bStmtNext);
            }
            return batch;
        } else {
            return contextualBindings.bind(cycle);
        }
    }

    public ResultSetCycleOperator[] getResultSetOperators() {
        return resultSetOperators;
    }

    public ContextualArrayBindings getContextualBindings() {
        return this.contextualBindings;
    }

    public String getQueryString(long value) {
        Object stmt = contextualBindings.getContext();
        if (stmt instanceof PreparedStatement) {
            String queryString = ((PreparedStatement)stmt).getQueryString();
            StringBuilder sb = new StringBuilder(queryString.length()*2);
            sb.append("(prepared) ");
            return getQueryStringValues(value, queryString, sb);
        } else if (stmt instanceof SimpleStatement) {
            String queryString = ((SimpleStatement) stmt).getQueryString();
            StringBuilder sb = new StringBuilder();
            sb.append("(simple) ");
            return getQueryStringValues(value, queryString, sb);
        } else if (stmt instanceof BatchStatement) {
            StringBuffer queryString = new StringBuffer("BEGIN BATCH");
            Collection<Statement> stmts = ((BatchStatement)stmt).getStatements();
            for(int i = 0; i < stmts.size(); i++) {
                queryString.append(getQueryString(value * batchSize + i));
            }
            queryString.append("APPLY BATCH");
        }
        if (stmt instanceof String) {
            return (String)stmt;
        }
        throw new RuntimeException("context object not recognized for query string:" + stmt.getClass().getCanonicalName());
    }

    private String getQueryStringValues(long value, String queryString, StringBuilder sb) {
        if (!queryString.endsWith("\n")) {
            sb.append("\n");
        }
        sb.append(queryString).append(" VALUES[");
        Object[] all = contextualBindings.getBindings().getAll(value);
        String delim="";
        for (Object o : all) {
            sb.append(delim);
            delim=",";
            if (o == null) {
                sb.append("<null>");
            } else
            sb.append(o.toString());
        }
        sb.append("]");
        return sb.toString();
    }

    public long getRatio() {
        return ratio;
    }

    public void setRatio(long ratio) {
        this.ratio = ratio;
    }

    public void onStart() {
        if (startTimers != null) {
            ThreadLocalNamedTimers.TL_INSTANCE.get().start(startTimers);
        }
    }

    /**
     * This method should be called when an associated statement is executed successfully.
     *
     * @param cycleValue  The cycle associated with the execution.
     * @param nanoTime    The nanoTime duration of the execution.
     * @param rowsFetched The number of rows fetched for this cycle
     */
    public void onSuccess(long cycleValue, long nanoTime, long rowsFetched) {
        if (successTimer != null) {
            successTimer.update(nanoTime, TimeUnit.NANOSECONDS);
        }
        if (stopTimers != null) {
            ThreadLocalNamedTimers.TL_INSTANCE.get().stop(stopTimers);
        }
        if (rowsFetchedHisto != null) {
            rowsFetchedHisto.update(rowsFetched);
        }
        if (resultCsvWriter != null) {
            try {
                synchronized (resultCsvWriter) {
                    // <cycle>,(SUCCESS|FAILURE),<nanos>,<rowsfetched>,<errorname>\n
                    resultCsvWriter
                        .append(String.valueOf(cycleValue)).append(",")
                        .append("SUCCESS,")
                            .append(String.valueOf(nanoTime)).append(",")
                            .append(String.valueOf(rowsFetched))
                            .append(",NONE")
                            .append("\n");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * This method should be called when an associated statement is executed unsuccessfully.
     * It should be called only once per cycle in the case of execution error.
     * @param cycleValue The cycle associated with the erred execution.
     * @param resultNanos The nanoTime duration of the execution.
     * @param t The associated throwable
     */
    public void onError(long cycleValue, long resultNanos, Throwable t) {
        if (errorTimer != null) {
            errorTimer.update(resultNanos, TimeUnit.NANOSECONDS);
        }
        if (stopTimers != null) {
            ThreadLocalNamedTimers.TL_INSTANCE.get().stop(stopTimers);
        }
        if (resultCsvWriter != null) {
            try {
                synchronized (resultCsvWriter) {
                    // <cycle>,(SUCCESS|FAILURE),<nanos>,<rowsfetched>,<errorname>\n
                    resultCsvWriter
                        .append(String.valueOf(cycleValue)).append(",")
                        .append("FAILURE,")
                        .append(String.valueOf(resultNanos)).append(",")
                        .append("0,")
                        .append(t.getClass().getSimpleName()).append(",")
                            .append("\n");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


    }

    public ReadyCQLStatement withResultSetCycleOperators(ResultSetCycleOperator[] resultSetCycleOperators) {
        this.resultSetOperators = resultSetCycleOperators;
        return this;
    }

    public ReadyCQLStatement withRowCycleOperators(RowCycleOperator[] rowCycleOperators) {
        this.rowCycleOperators = rowCycleOperators;
        return this;
    }

    public RowCycleOperator[] getRowCycleOperators() {
        return this.rowCycleOperators;
    }

    public ReadyCQLStatement withResultCsvWriter(Writer resultCsvWriter) {
        this.resultCsvWriter = resultCsvWriter;
        return this;
    }

    public ReadyCQLStatement withStartTimers(List<String> startTimers) {
        this.startTimers = startTimers;
        return this;
    }

    public ReadyCQLStatement withStopTimers(List<String> stopTimers) {
        this.stopTimers = stopTimers;
        return this;
    }

    public String toString() {
        return "ReadyCQLStatement: " + contextualBindings.toString();
    }
}
