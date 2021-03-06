/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2tools.jaqu;

/**
 * An OR or an AND condition.
 */
enum ConditionAndOr implements Token {
    AND("AND"),
    OR("OR");

    private String text;

    ConditionAndOr(String text) {
        this.text = text;
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        stat.appendSQL(text);
    }

}
