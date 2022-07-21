package io.nosqlbench.activitytype.ysql;

import com.codahale.metrics.Timer;
import io.nosqlbench.activitytype.jdbc.impl.JDBCAction;
import io.nosqlbench.engine.api.activityapi.core.Shutdownable;
import io.nosqlbench.engine.api.activityapi.core.SyncAction;
import io.nosqlbench.engine.api.activityapi.errorhandling.modular.ErrorDetail;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.engine.api.activityimpl.OpDispenser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

public class MYSQLAction extends JDBCAction implements SyncAction, Shutdownable {
    private static final Logger LOGGER = LogManager.getLogger(MYSQLAction.class);

    private final MYSQLActivity activity;
    private OpSequence<OpDispenser<String>> sequencer;
    private final ThreadLocal<Connection> threadLocalConn = new ThreadLocal<Connection>();

    public MYSQLAction(MYSQLActivity a, int slot) {
        
        super(a, slot);
        activity = a;
    }

    @Override
    public void init() {
        sequencer = activity.getOpSequence();
        try {
			threadLocalConn.set( activity.getDataSource().getConnection());
			LOGGER.info("created local connection for "+Thread.currentThread().getId());
		} catch (SQLException e) {
			LOGGER.error("Exception while creating connection:"+e);
		}
    }

    @Override
    public int runCycle(long cycle) {
        String boundStmt;

        LongFunction<String> unboundStmt = sequencer.apply(cycle);

        try (Timer.Context bindTime = activity.getBindTimer().time()) {
            boundStmt = unboundStmt.apply(cycle);
        }

        int maxTries = activity.getMaxTries();
        Exception error = null;

        for (int tries = 1; tries <= maxTries; tries++) {
            long startTimeNanos = System.nanoTime();

            try  {
            	Connection conn = threadLocalConn.get();
                Statement jdbcStmt = conn.createStatement();
                jdbcStmt.execute(boundStmt);

            } catch (Exception e) {
                error = e;
            }

            long executionTimeNanos = System.nanoTime() - startTimeNanos;
            //LOGGER.info("executionTimeNanos "+Thread.currentThread().getId()+"--"+executionTimeNanos);

            activity.getResultTimer().update(executionTimeNanos, TimeUnit.NANOSECONDS);
            activity.getTriesHisto().update(tries);

            if (error == null) {
                activity.getResultSuccessTimer().update(executionTimeNanos, TimeUnit.NANOSECONDS);
                return 0;
            } else {
                ErrorDetail detail = activity.getErrorHandler().handleError(error, cycle, executionTimeNanos);
                if (!detail.isRetryable()) {
                    LOGGER.debug("Exit failure after non-retryable error");
                    throw new RuntimeException("non-retryable error", error);
                }
            }

            try {
                int retryDelay = retryDelayMs(tries, activity.getMinRetryDelayMs());
                LOGGER.debug("tries=" + tries + " sleeping for " + retryDelay + " ms");
                Thread.sleep(retryDelay);
            } catch (InterruptedException e) {
                throw new RuntimeException("thread interrupted", e);
            }
        }

        LOGGER.debug("Exit failure after maxretries=" + maxTries);
        throw new RuntimeException("maxtries exceeded", error);
    }

    /**
     * Compute retry delay based on exponential backoff with full jitter
     * @param tries 1-indexed
     * @param minDelayMs lower bound of retry delay
     * @return retry delay
     */
    private int retryDelayMs(int tries, int minDelayMs) {
        int exponentialDelay = minDelayMs * (int) Math.pow(2.0, tries - 1);
        return (int) (Math.random() * exponentialDelay);
    }

	@Override
	public void shutdown() {
		try {
			LOGGER.info("closing session for thread "+Thread.currentThread().getId());
			threadLocalConn.get().close();
		} catch (SQLException e) {
			LOGGER.error("Exception while closing connection:"+e);
		};
		
	}
}
