package io.nosqlbench.activitytype.ycql.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;
import io.nosqlbench.activitytype.ycql.api.ResultSetCycleOperator;
import io.nosqlbench.activitytype.ycql.api.RowCycleOperator;
import io.nosqlbench.activitytype.ycql.api.StatementFilter;
import io.nosqlbench.activitytype.ycql.errorhandling.ErrorStatus;
import io.nosqlbench.activitytype.ycql.errorhandling.HashedCQLErrorHandler;
import io.nosqlbench.activitytype.ycql.errorhandling.exceptions.CQLCycleWithStatementException;
import io.nosqlbench.activitytype.ycql.errorhandling.exceptions.ChangeUnappliedCycleException;
import io.nosqlbench.activitytype.ycql.errorhandling.exceptions.MaxTriesExhaustedException;
import io.nosqlbench.activitytype.ycql.errorhandling.exceptions.UnexpectedPagingException;
import io.nosqlbench.activitytype.ycql.statements.core.ReadyCQLStatement;
import io.nosqlbench.activitytype.ycql.statements.modifiers.StatementModifier;
import io.nosqlbench.engine.api.activityapi.core.ActivityDefObserver;
import io.nosqlbench.engine.api.activityapi.core.MultiPhaseAction;
import io.nosqlbench.engine.api.activityapi.core.SyncAction;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("Duplicates")
public class CqlAction implements SyncAction, MultiPhaseAction, ActivityDefObserver {

    private final static Logger logger = LogManager.getLogger(CqlAction.class);
    private final int slot;
    private final CqlActivity cqlActivity;
    private final ActivityDef activityDef;
    private List<RowCycleOperator> rowOps;
    private List<ResultSetCycleOperator> cycleOps;
    private List<StatementModifier> modifiers;
    private StatementFilter statementFilter;
    private OpSequence<ReadyCQLStatement> sequencer;
    private int maxTries = 10;    // how many cycles a statement will be attempted for before giving up

    private HashedCQLErrorHandler ebdseErrorHandler;

    private int pagesFetched = 0;
    private long totalRowsFetchedForQuery = 0L;
    private ResultSet pagingResultSet;
    private Statement pagingStatement;
    private ReadyCQLStatement pagingReadyStatement;
    private boolean showcql;
    private long nanoStartTime;
    private long retryDelay;
    private long maxRetryDelay;
    private boolean retryReplace;
    private Timer bindTimer;
    private Timer executeTimer;
    private Timer resultTimer;
    private Timer resultSuccessTimer;
    private Histogram triesHisto;

    public CqlAction(ActivityDef activityDef, int slot, CqlActivity cqlActivity) {
        this.activityDef = activityDef;
        this.cqlActivity = cqlActivity;
        this.slot = slot;
        onActivityDefUpdate(activityDef);
    }

    @Override
    public void init() {
        onActivityDefUpdate(activityDef);
        this.sequencer = cqlActivity.getOpSequencer();
        this.bindTimer = cqlActivity.getInstrumentation().getOrCreateBindTimer();
        this.executeTimer = cqlActivity.getInstrumentation().getOrCreateExecuteTimer();
        this.resultTimer = cqlActivity.getInstrumentation().getOrCreateResultTimer();
        this.resultSuccessTimer = cqlActivity.getInstrumentation().getOrCreateResultSuccessTimer();
        this.triesHisto = cqlActivity.getInstrumentation().getOrCreateTriesHistogram();
    }

    @Override
    public int runCycle(long cycle) {
        // In this activity type, we use the same phase
        // logic for the initial phase (runCycle(...))
        // as well as subsequent phases.
        return runPhase(cycle);
    }

    public int runPhase(long cycleValue) {

        HashedCQLErrorHandler.resetThreadStatusCode();

        if (pagingResultSet == null) {

            totalRowsFetchedForQuery = 0L;

            Statement statement;
            ResultSetFuture resultSetFuture;
            ReadyCQLStatement readyCQLStatement;

            int tries = 0;

            try (Timer.Context bindTime = bindTimer.time()) {
                readyCQLStatement = sequencer.get(cycleValue);
                readyCQLStatement.onStart();

                statement = readyCQLStatement.bind(cycleValue);


                if (statementFilter != null) {
                    if (!statementFilter.matches(statement)) {
                        cqlActivity.skippedTokensHisto.update(cycleValue);
                        return 0;
                    }
                }

                if (modifiers != null) {
                    for (StatementModifier modifier : modifiers) {
                        statement = modifier.modify(statement, cycleValue);
                    }
                }

                if (showcql) {
                    logger.info("CQL(cycle=" + cycleValue + "):\n" + readyCQLStatement.getQueryString(cycleValue));
                }
            }
            nanoStartTime = System.nanoTime();

            while (tries < maxTries) {
                tries++;

                if (tries > maxTries) {
                    throw new MaxTriesExhaustedException(cycleValue, maxTries);
                }

                if (tries > 1) {
                    try (Timer.Context retryTime = cqlActivity.retryDelayTimer.time()) {
                        Thread.sleep(Math.min((retryDelay << tries) / 1000, maxRetryDelay / 1000));
                    } catch (InterruptedException ignored) {
                    }
                }

                try (Timer.Context executeTime = executeTimer.time()) {
                    resultSetFuture = cqlActivity.getSession().executeAsync(statement);
                }

                Timer.Context resultTime = resultTimer.time();
                try {
                    ResultSet resultSet = resultSetFuture.getUninterruptibly();

                    if (cycleOps != null) {
                        for (ResultSetCycleOperator cycleOp : cycleOps) {
                            cycleOp.apply(resultSet, statement, cycleValue);
                        }
                    }

                    ResultSetCycleOperator[] perStmtRSOperators = readyCQLStatement.getResultSetOperators();
                    if (perStmtRSOperators != null) {
                        for (ResultSetCycleOperator perStmtRSOperator : perStmtRSOperators) {
                            perStmtRSOperator.apply(resultSet, statement, cycleValue);
                        }
                    }

                    if (!resultSet.wasApplied()) {
                        //resultSet.b
                        Row row = resultSet.one();
                        ColumnDefinitions defs = row.getColumnDefinitions();
                        if (retryReplace) {
                            statement = CQLBindHelper.rebindUnappliedStatement(statement, defs, row);
                        }

                        logger.trace(readyCQLStatement.getQueryString(cycleValue));
                        // To make exception handling logic flow more uniformly
                        throw new ChangeUnappliedCycleException(
                                cycleValue, resultSet, readyCQLStatement.getQueryString(cycleValue)
                        );
                    }

                    int pageRows = resultSet.getAvailableWithoutFetching();
                    int remaining = pageRows;
                    RowCycleOperator[] perStmtRowOperators = readyCQLStatement.getRowCycleOperators();
                    if (rowOps == null && perStmtRowOperators==null) {
                        while (remaining-- > 0) {
                            Row row = resultSet.one();

//                            NOTE: This has been replaced by:
//                            params:
//                              rowops: savevars
//                              You must add this to the YAML for statements that are meant to capture vars
//                            HashMap<String, Object> bindings = SharedState.tl_ObjectMap.get();
//                            for (ColumnDefinitions.Definition cdef : row.getColumnDefinitions()) {
//                                bindings.put(cdef.getName(), row.getObject(cdef.getName()));
//                            }
//
                        }
                    } else {
                        while (remaining-- > 0) {
                            Row onerow = resultSet.one();
                            if (rowOps!=null) {
                                for (RowCycleOperator rowOp : rowOps) {
                                    rowOp.apply(onerow, cycleValue);
                                }
                            }
                            if (perStmtRowOperators!=null) {
                                for (RowCycleOperator rowOp : perStmtRowOperators) {
                                    rowOp.apply(onerow, cycleValue);
                                }
                            }
                        }
                    }
                    cqlActivity.rowsCounter.mark(pageRows);
                    totalRowsFetchedForQuery += pageRows;

                    if (resultSet.isFullyFetched()) {
                        long resultNanos = System.nanoTime() - nanoStartTime;
                        resultSuccessTimer.update(resultNanos, TimeUnit.NANOSECONDS);
                        cqlActivity.resultSetSizeHisto.update(totalRowsFetchedForQuery);
                        readyCQLStatement.onSuccess(cycleValue, resultNanos, totalRowsFetchedForQuery);
                    } else {
                        if (cqlActivity.maxpages > 1) {
                            pagingResultSet = resultSet;
                            pagingStatement = statement;
                            pagingReadyStatement = readyCQLStatement;
                            pagesFetched = 1;
                        } else {
                            throw new UnexpectedPagingException(
                                    cycleValue,
                                    resultSet,
                                    readyCQLStatement.getQueryString(cycleValue),
                                    1,
                                    cqlActivity.maxpages,
                                    cqlActivity.getSession().getCluster().getConfiguration().getQueryOptions().getFetchSize()
                            );
                        }
                    }
                    break; // This is normal termination of this loop, when retries aren't needed
                } catch (Exception e) {
                    long resultNanos = resultTime.stop();
                    resultTime = null;
                    readyCQLStatement.onError(cycleValue, resultNanos, e);
                    CQLCycleWithStatementException cqlCycleException = new CQLCycleWithStatementException(cycleValue, resultNanos, e, readyCQLStatement);
                    ErrorStatus errorStatus = ebdseErrorHandler.handleError(cycleValue, cqlCycleException);
                    if (!errorStatus.isRetryable()) {
                        triesHisto.update(tries);
                        return errorStatus.getResultCode();
                    }
                } finally {
                    if (resultTime != null) {
                        resultTime.stop();
                    }
                }
            }
            triesHisto.update(tries);

        } else {

            int tries = 0;

            while (tries < maxTries) {
                tries++;
                if (tries > maxTries) {
                    throw new MaxTriesExhaustedException(cycleValue, maxTries);
                }

                ListenableFuture<ResultSet> pagingFuture;

                try (Timer.Context pagingTime = cqlActivity.pagesTimer.time()) {
                    try (Timer.Context executeTime = executeTimer.time()) {
                        pagingFuture = pagingResultSet.fetchMoreResults();
                    }

                    Timer.Context resultTime = resultTimer.time();
                    try {
                        ResultSet resultSet = pagingFuture.get();

                        if (cycleOps != null) {
                            for (ResultSetCycleOperator cycleOp : cycleOps) {
                                cycleOp.apply(resultSet, pagingStatement, cycleValue);
                            }
                        }
                        ResultSetCycleOperator[] perStmtRSOperators = pagingReadyStatement.getResultSetOperators();
                        if (perStmtRSOperators != null) {
                            for (ResultSetCycleOperator perStmtRSOperator : perStmtRSOperators) {
                                perStmtRSOperator.apply(resultSet, pagingStatement, cycleValue);
                            }
                        }

                        pagesFetched++;

                        int pageRows = resultSet.getAvailableWithoutFetching();
                        int remaining = pageRows;
                        if (rowOps == null) {
                            while (remaining-- > 0) {
                                resultSet.one();
                            }
                        } else {
                            while (remaining-- > 0) {
                                for (RowCycleOperator rowOp : rowOps) {
                                    rowOp.apply(resultSet.one(), cycleValue);

                                }
                            }
                        }
                        cqlActivity.rowsCounter.mark(pageRows);
                        totalRowsFetchedForQuery += pageRows;

                        if (resultSet.isFullyFetched()) {
                            long nanoTime = System.nanoTime() - nanoStartTime;
                            resultSuccessTimer.update(nanoTime, TimeUnit.NANOSECONDS);
                            cqlActivity.resultSetSizeHisto.update(totalRowsFetchedForQuery);
                            pagingReadyStatement.onSuccess(cycleValue, nanoTime, totalRowsFetchedForQuery);
                            pagingResultSet = null;

                        } else {
                            if (pagesFetched > cqlActivity.maxpages) {
                                throw new UnexpectedPagingException(
                                        cycleValue,
                                        pagingResultSet,
                                        pagingReadyStatement.getQueryString(cycleValue),
                                        pagesFetched,
                                        cqlActivity.maxpages,
                                        cqlActivity.getSession().getCluster().getConfiguration().getQueryOptions().getFetchSize()
                                );
                            }
                            pagingResultSet = resultSet;
                        }
                        break; // This is normal termination of this loop, when retries aren't needed
                    } catch (Exception e) {
                        long resultNanos = resultTime.stop();
                        resultTime = null;

                        pagingReadyStatement.onError(cycleValue, resultNanos, e);
                        CQLCycleWithStatementException cqlCycleException = new CQLCycleWithStatementException(cycleValue, resultNanos, e, pagingReadyStatement);
                        ErrorStatus errorStatus = ebdseErrorHandler.handleError(cycleValue, cqlCycleException);
                        if (!errorStatus.isRetryable()) {
                            triesHisto.update(tries);
                            return errorStatus.getResultCode();
                        }
                    } finally {
                        if (resultTime != null) {
                            resultTime.stop();
                        }
                    }
                }
            }
            triesHisto.update(tries);
        }
        return 0;
    }


    @Override
    public boolean incomplete() {
        return pagingResultSet != null;
    }

    @Override
    public void onActivityDefUpdate(ActivityDef activityDef) {
        this.maxTries = cqlActivity.getMaxTries();
        this.retryDelay = cqlActivity.getRetryDelay();
        this.maxRetryDelay = cqlActivity.getMaxRetryDelay();
        this.retryReplace = cqlActivity.isRetryReplace();
        this.showcql = cqlActivity.isShowCql();
        this.ebdseErrorHandler = cqlActivity.getCqlErrorHandler();
        this.statementFilter = cqlActivity.getStatementFilter();
        this.rowOps = cqlActivity.getRowCycleOperators();
        this.cycleOps = cqlActivity.getResultSetCycleOperators();
        this.modifiers = cqlActivity.getStatementModifiers();
    }

    protected CqlActivity getCqlActivity() {
        return cqlActivity;
    }


}
