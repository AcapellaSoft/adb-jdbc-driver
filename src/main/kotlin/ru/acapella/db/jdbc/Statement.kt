package ru.acapella.db.jdbc

import ru.acapella.db.grpc.SqlExecuteRequestPb
import ru.acapella.db.grpc.SqlPrepareRequestPb
import ru.acapella.db.grpc.SqlQueryRequestPb
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement

open class Statement(private val connection: Connection) : Statement {
    private var fetchSize = 0
    private var closed = false

    override fun setMaxFieldSize(max: Int) = throw SQLFeatureNotSupportedException()
    override fun setFetchDirection(direction: Int) = throw SQLFeatureNotSupportedException()
    override fun setCursorName(name: String?) = throw SQLFeatureNotSupportedException()
    override fun closeOnCompletion() = throw SQLFeatureNotSupportedException()
    override fun executeBatch() = throw SQLFeatureNotSupportedException()
    override fun getGeneratedKeys() = throw SQLFeatureNotSupportedException()
    override fun getResultSet() = throw SQLFeatureNotSupportedException()
    override fun executeUpdate(sql: String?, columnIndexes: IntArray?) = throw SQLFeatureNotSupportedException()
    override fun executeUpdate(sql: String?, columnNames: Array<out String>?) = throw SQLFeatureNotSupportedException()
    override fun clearBatch() = throw SQLFeatureNotSupportedException()
    override fun isCloseOnCompletion() = throw SQLFeatureNotSupportedException()
    override fun getMaxRows() = throw SQLFeatureNotSupportedException()
    override fun setMaxRows(max: Int) = throw SQLFeatureNotSupportedException()
    override fun setEscapeProcessing(enable: Boolean) = throw SQLFeatureNotSupportedException()
    override fun execute(sql: String?, columnIndexes: IntArray?) = throw SQLFeatureNotSupportedException()
    override fun execute(sql: String?, columnNames: Array<out String>?) = throw SQLFeatureNotSupportedException()
    override fun setPoolable(poolable: Boolean) = throw SQLFeatureNotSupportedException()
    override fun addBatch(sql: String?) = throw SQLFeatureNotSupportedException()
    override fun setQueryTimeout(seconds: Int) = throw SQLFeatureNotSupportedException()
    override fun getMoreResults() = throw SQLFeatureNotSupportedException()
    override fun getMoreResults(current: Int) = throw SQLFeatureNotSupportedException()

    override fun cancel() {}
    override fun getResultSetType() = ResultSet.TYPE_FORWARD_ONLY
    override fun getConnection() = connection
    override fun getWarnings() = null
    override fun getFetchSize() = fetchSize
    override fun isClosed() = closed
    override fun getMaxFieldSize() = 0
    override fun getUpdateCount() = 0
    override fun getQueryTimeout() = 0
    override fun isPoolable() = false
    override fun getResultSetConcurrency() = ResultSet.CONCUR_READ_ONLY
    override fun clearWarnings() {}
    override fun getFetchDirection() = ResultSet.FETCH_FORWARD
    override fun getResultSetHoldability() = ResultSet.CLOSE_CURSORS_AT_COMMIT

    override fun close() {
        closed = true
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> unwrap(iface: Class<T>): T {
        if (isWrapperFor(iface)) return this as T
        throw SQLException()
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.PreparedStatement::class.java
    }

    override fun setFetchSize(rows: Int) {
        fetchSize = rows
    }

    override fun executeLargeUpdate(sql: String): Long = convertError {
        connection.withTransaction { tx ->
            val requestBuilder = SqlExecuteRequestPb.newBuilder()
                .setSql(sql)
            if (tx != null) requestBuilder.transaction = tx
            val response = connection.sqlService.execute(requestBuilder.build())
            response.rowsCount
        }
    }

    override fun executeQuery(sql: String) = convertError {
        // todo remove prepare call
        connection.withTransaction { tx ->
            val prepareResponse = connection.sqlService.prepare(SqlPrepareRequestPb.newBuilder()
                .setSql(sql)
                .build())
            val requestBuilder = SqlQueryRequestPb.newBuilder()
                .setSql(sql)
            if (tx != null) requestBuilder.transaction = tx
            if (fetchSize != 0) requestBuilder.fetchSize = fetchSize
            val response = connection.sqlService.query(requestBuilder.build())
            StatementResultSet(response, this, ResultSetMetaData(prepareResponse.columnsList))
        }
    }

    override fun executeUpdate(sql: String): Int {
        return executeLargeUpdate(sql).toInt()
    }

    override fun executeUpdate(sql: String, autoGeneratedKeys: Int): Int {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) throw SQLFeatureNotSupportedException()
        return executeUpdate(sql)
    }

    override fun execute(sql: String): Boolean {
        executeLargeUpdate(sql)
        return false
    }

    override fun execute(sql: String, autoGeneratedKeys: Int): Boolean {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) throw SQLFeatureNotSupportedException()
        return execute(sql)
    }
}