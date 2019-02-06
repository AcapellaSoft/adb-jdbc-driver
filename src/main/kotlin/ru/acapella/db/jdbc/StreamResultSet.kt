package ru.acapella.db.jdbc

import io.grpc.stub.StreamObserver
import ru.acapella.db.grpc.SqlQueryFetchPb
import ru.acapella.db.grpc.SqlQueryMessagePb
import ru.acapella.db.grpc.SqlResultSetPb
import ru.acapella.db.grpc.SqlVariantPb
import java.sql.SQLException
import java.util.concurrent.ArrayBlockingQueue

class StreamResultSet(
    private val sendStream: StreamObserver<SqlQueryMessagePb>,
    private val receiveQueue: ArrayBlockingQueue<Result<SqlResultSetPb>>,
    private val statement: ru.acapella.db.jdbc.Statement
) : VariantResultSet(statement.fetchSize) {
    private var resultSet: SqlResultSetPb = receiveQueue.take().getOrThrow()
    private val meta = ResultSetMetaData(resultSet.columnsList)
    private var state = State.BEFORE_FIRST
    private var rowIndex = -1

    enum class State {
        BEFORE_FIRST,
        FIRST,
        INNER,
        LAST,
        AFTER_LAST
    }

    override val row
        get(): List<SqlVariantPb> {
            if (state == State.BEFORE_FIRST) throw SQLException("Cursor is before first row")
            if (state == State.AFTER_LAST) throw SQLException("Cursor is after last row")
            return resultSet.rowsList[rowIndex].fieldsList
        }

    override fun getStatement() = statement
    override fun getMetaData() = meta
    override fun columnType(index: Int) = meta.getColumnType(index)

    override fun findColumn(columnLabel: String): Int {
        return (1..meta.columnCount)
            .first { meta.getColumnLabel(it) == columnLabel }
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.StatementResultSet::class.java
    }

    override fun isFirst() = state == State.FIRST
    override fun isLast() = state == State.LAST
    override fun isAfterLast() = state == State.AFTER_LAST
    override fun isBeforeFirst() = state == State.BEFORE_FIRST

    override fun next(): Boolean {
        rowIndex += 1

        if (rowIndex >= resultSet.rowsCount && resultSet.hasMore) {
            sendStream.onNext(SqlQueryMessagePb.newBuilder()
                .setFetch(SqlQueryFetchPb.newBuilder()
                    .setFetchSize(fetchSize))
                .build())
            resultSet = receiveQueue.take().getOrThrow()
            rowIndex = -1
            return next()
        }

        val hasCurrent = rowIndex < resultSet.rowsCount
        state = when {
            state == State.BEFORE_FIRST -> State.FIRST
            state == State.FIRST -> State.INNER
            state == State.INNER && rowIndex == resultSet.rowsCount - 1 && !resultSet.hasMore -> State.LAST
            state == State.LAST -> State.AFTER_LAST
            else -> state
        }

        return hasCurrent
    }

    override fun close() {
        super.close()
        sendStream.onCompleted()
    }
}