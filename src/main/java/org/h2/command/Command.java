/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.util.MathUtils;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Represents a SQL statement. This object is only used on the server side.
 */
public abstract class Command implements CommandInterface {
    /**
     * The session.
     */
    protected final Session session;

    /**
     * The last start time.
     */
    protected long startTimeNanos;

    /**
     * The trace module.
     */
    protected final Trace trace;

    private final String sql;

    private boolean canReuse;

    private Command upstream;

    private Command downstream;

    // For update
    protected int affectedRows = -1;

    // For query
    protected ResultInterface result;

    /**
     * If this query was canceled.
     */
    protected volatile boolean cancel;

    Command(Session session, String sql) {
        this.session = session;
        this.sql = sql;
        this.trace = session.getDatabase().getTrace(Trace.COMMAND);
    }

    /**
     * Check if this command is transactional.
     * If it is not, then it forces the current transaction to commit.
     *
     * @return true if it is
     */
    public abstract boolean isTransactional();

    /**
     * Check if this command is a query.
     *
     * @return true if it is
     */
    @Override
    public abstract boolean isQuery();

    /**
     * Prepare join batching.
     */
    public abstract void prepareJoinBatch();

    /**
     * Get the list of parameters.
     *
     * @return the list of parameters
     */
    @Override
    public abstract ArrayList<? extends ParameterInterface> getParameters();

    /**
     * Check if this command is read only.
     *
     * @return true if it is
     */
    public abstract boolean isReadOnly();

    /**
     * Get an empty result set containing the meta data.
     *
     * @return an empty result set
     */
    public abstract ResultInterface queryMeta();

    public boolean hasDownstream() {
        return false;
    }

    public Command getUpstream() {
        return upstream;
    }

    public void setUpstream(Command upstream) {
        this.upstream = upstream;
    }

    public Command getDownstream() {
        return downstream;
    }

    public void setDownstream(Command downstream) {
        this.downstream = downstream;
    }

    public String getSql() {
        return sql;
    }

    public void execute(int maxrows) {
        if (isQuery()) {
            query(maxrows);
        } else {
            update();
        }
    }

    /**
     * Execute an updating statement (for example insert, delete, or update), if
     * this is possible.
     *
     * @return the update count
     * @throws DbException if the command is not an updating statement
     */
    public int update() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute a query statement, if this is possible.
     *
     * @param maxrows the maximum number of rows returned
     * @return the local result set
     * @throws DbException if the command is not a query
     */
    public ResultInterface query(@SuppressWarnings("unused") int maxrows) {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    @Override
    public final ResultInterface getMetaData() {
        return queryMeta();
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(int affectedRows) {
        this.affectedRows = affectedRows;
    }

    public ResultInterface getResult() {
        return result;
    }

    public void setResult(ResultInterface result) {
        this.result = result;
    }

    /**
     * Start the stopwatch.
     */
    void start() {
        if (trace.isInfoEnabled() || session.getDatabase().getQueryStatistics()) {
            startTimeNanos = System.nanoTime();
        }
    }

    void setProgress(int state) {
        session.getDatabase().setProgress(state, sql, 0, 0);
    }

    /**
     * Check if this command has been canceled, and throw an exception if yes.
     *
     * @throws DbException if the statement has been canceled
     */
    protected void checkCanceled() {
        if (cancel) {
            cancel = false;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    @Override
    public void stop() {
        session.endStatement();
        session.setCurrentCommand(null);
        if (!isTransactional()) {
            session.commit(true);
        } else if (session.getAutoCommit()) {
            session.commit(false);
        } else if (session.getDatabase().isMultiThreaded()) {
            Database db = session.getDatabase();
            if (db != null) {
                if (db.getLockMode() == Constants.LOCK_MODE_READ_COMMITTED) {
                    session.unlockReadLocks();
                }
            }
        }
        if (trace.isInfoEnabled() && startTimeNanos > 0) {
            long timeMillis = (System.nanoTime() - startTimeNanos) / 1000 / 1000;
            if (timeMillis > Constants.SLOW_QUERY_LIMIT_MS) {
                trace.info("slow query: {0} ms", timeMillis);
            }
        }
    }

    /**
     * Execute a query and return the result.
     * This method prepares everything and calls {@link #query(int)} finally.
     *
     * @param maxrows the maximum number of rows to return
     * @param scrollable if the result set must be scrollable (ignored)
     * @return the result set
     */
    @Override
    public ResultInterface executeQuery(int maxrows, boolean scrollable) {
        // System.out.println("\n\n\n==Command.executeQuery==>>>");
        startTimeNanos = 0;
        long start = 0;
        Database database = session.getDatabase();
        Object sync = database.isMultiThreaded() ? session : database;
        session.waitIfExclusiveModeEnabled();
        boolean callStop = true;
        boolean writing = !isReadOnly();
        if (writing) {
            while (!database.beforeWriting()) {
                // wait
            }
        }

        synchronized (sync) {
            session.setCurrentCommand(this);
            try {
                while (true) {
                    database.checkPowerOff();
                    try {
                        // Code execution flow:
                        // Select.queryFlat -> Session.prepareCommand -> Parser.Parser -> Parser.prepareCommand
                        // -> Select.prepare -> Select.preparePlan -> TableFilter.prepare
                        // The result of the first query
                        this.result = query(maxrows);

                        callStop = !result.isLazy();
                        return result;
                    } catch (DbException e) {
                        start = filterConcurrentUpdate(e, start);
                    } catch (OutOfMemoryError e) {
                        callStop = false;
                        // there is a serious problem:
                        // the transaction may be applied partially
                        // in this case we need to panic:
                        // close the database
                        database.shutdownImmediately();
                        throw DbException.convert(e);
                    } catch (Throwable e) {
                        throw DbException.convert(e);
                    }
                }
            } catch (DbException e) {
                e = e.addSQL(sql);
                SQLException s = e.getSQLException();
                database.exceptionThrown(s, sql);
                if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                    callStop = false;
                    database.shutdownImmediately();
                    throw e;
                }
                database.checkPowerOff();
                throw e;
            } finally {
                if (callStop) {
                    stop();
                }
                if (writing) {
                    database.afterWriting();
                }
            }
        }
    }

    @Override
    public int executeUpdate() {
        long start = 0;
        Database database = session.getDatabase();
        Object sync = database.isMultiThreaded() ? session : database;
        session.waitIfExclusiveModeEnabled();
        boolean callStop = true;
        boolean writing = !isReadOnly();
        if (writing) {
            while (!database.beforeWriting()) {
                // wait
            }
        }

        synchronized (sync) {
            Session.Savepoint rollback = session.setSavepoint();
            session.setCurrentCommand(this);
            try {
                while (true) {
                    database.checkPowerOff();
                    try {
                        this.affectedRows = update();
                        return affectedRows;
                    } catch (DbException e) {
                        start = filterConcurrentUpdate(e, start);
                    } catch (OutOfMemoryError e) {
                        callStop = false;
                        database.shutdownImmediately();
                        throw DbException.convert(e);
                    } catch (Throwable e) {
                        throw DbException.convert(e);
                    }
                }
            } catch (DbException e) {
                e = e.addSQL(sql);
                SQLException s = e.getSQLException();
                database.exceptionThrown(s, sql);
                if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                    callStop = false;
                    database.shutdownImmediately();
                    throw e;
                }
                database.checkPowerOff();
                if (s.getErrorCode() == ErrorCode.DEADLOCK_1) {
                    session.rollback();
                } else {
                    session.rollbackTo(rollback, false);
                }
                throw e;
            } finally {
                try {
                    if (callStop) {
                        stop();
                    }
                } finally {
                    if (writing) {
                        database.afterWriting();
                    }
                }
            }
        }
    }

    private long filterConcurrentUpdate(DbException e, long start) {
        int errorCode = e.getErrorCode();
        if (errorCode != ErrorCode.CONCURRENT_UPDATE_1 &&
                errorCode != ErrorCode.ROW_NOT_FOUND_IN_PRIMARY_INDEX &&
                errorCode != ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1) {
            throw e;
        }
        long now = System.nanoTime() / 1000000;
        if (start != 0 && now - start > session.getLockTimeout()) {
            throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, e.getCause(), "");
        }
        Database database = session.getDatabase();
        int sleep = 1 + MathUtils.randomInt(10);
        while (true) {
            try {
                if (database.isMultiThreaded()) {
                    Thread.sleep(sleep);
                } else {
                    database.wait(sleep);
                }
            } catch (InterruptedException e1) {
                // ignore
            }
            long slept = System.nanoTime() / 1000000 - now;
            if (slept >= sleep) {
                break;
            }
        }
        return start == 0 ? now : start;
    }

    @Override
    public void close() {
        canReuse = true;
    }

    @Override
    public void cancel() {
        this.cancel = true;
    }

    @Override
    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    public boolean isCacheable() {
        return false;
    }

    /**
     * Whether the command is already closed (in which case it can be re-used).
     *
     * @return true if it can be re-used
     */
    public boolean canReuse() {
        return canReuse;
    }

    /**
     * The command is now re-used, therefore reset the canReuse flag, and the
     * parameter values.
     */
    public void reuse() {
        canReuse = false;
        ArrayList<? extends ParameterInterface> parameters = getParameters();
        for (ParameterInterface param : parameters) {
            param.setValue(null, true);
        }
    }

    public void setCanReuse(boolean canReuse) {
        this.canReuse = canReuse;
    }
}
