package com.datastax.loader.futures;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.ResultSet;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;

public class ActionFutureSet extends AbstractFutureManager {
    protected FutureAction futureAction = null;
    protected Semaphore available;
    protected AtomicLong insertErrors;
    protected AtomicLong numInserted;

    public ActionFutureSet(int inSize, long inQueryTimeout, 
                           long inMaxInsertErrors, 
                           FutureAction inFutureAction) {
        super(inSize, inQueryTimeout, inMaxInsertErrors);
        futureAction = inFutureAction;
        available = new Semaphore(size, true);
        insertErrors = new AtomicLong(0);
        numInserted = new AtomicLong(0);
    }

    public boolean add(ResultSetFuture future, final String line) {
        if (maxInsertErrors <= insertErrors.get()) {
            System.err.println(String.format("ActionFutureSet: maxInsertErrors=%d exceeded insertErrors=%d", maxInsertErrors, insertErrors.get()));
            return false;
        }
        try {
            available.acquire();
        }
        catch (InterruptedException e) {
            System.err.println(String.format("ActionFutureSet: InterruptedExceptiond"));
            return false;
        }

        Futures.addCallback(future, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet rs) {
                    available.release();
                    numInserted.incrementAndGet();
                    futureAction.onSuccess(rs, line);
                }
                @Override
                public void onFailure(Throwable t) {
                    long numErrors = insertErrors.incrementAndGet();
                    java.io.StringWriter sw = new java.io.StringWriter();
                    java.io.PrintWriter pw = new java.io.PrintWriter(sw);
                    t.printStackTrace(pw);
                    System.err.println(String.format("ActionFutureSet: onFailure numErrors=%d stacktrace=%s", numErrors, sw.toString()));
                    System.err.println(String.format("ActionFutureSet: onFailure line=%s", line));
                    futureAction.onFailure(t, line);
                    if (maxInsertErrors <= numErrors) {
                        System.err.println(String.format("ActionFutureSet: onFailure calling onTooManyFailures"));
                        futureAction.onTooManyFailures();
                    }
                    available.release();
                }
            });
        return true;
    }

    public boolean cleanup() {
        try {
            available.acquire(this.size);
        } catch (InterruptedException e) {
            return false;
        }

        // check for too many errors
        if (maxInsertErrors <= insertErrors.get()) {
            System.err.println(String.format("ActionFutureSet: cleanup - too many errors: insertErrors=%d maxInsertErrors=%d", insertErrors.get(), maxInsertErrors));
            return false;
        }
       return true;
    }

    public long getNumInsertErrors() {
        return insertErrors.get();
    }

    public long getNumInserted() {
        return numInserted.get();
    }
}
