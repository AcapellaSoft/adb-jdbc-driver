package ru.acapella.db.jdbc

import ru.acapella.db.grpc.SqlResultSetPb
import java.sql.SQLException

class StatementResultSet(
    response: SqlResultSetPb,
    private val statement: Statement? = null,
    private val meta: ResultSetMetaData? = null
) : ListResultSet(response.rowsList.map { it.fieldsList }) {
    override fun getStatement() = statement
    override fun getMetaData() = meta
    override fun columnType(index: Int) = meta?.getColumnType(index) ?: 0

    override fun findColumn(columnLabel: String): Int {
        if (meta == null) throw SQLException()
        return (1..meta.columnCount)
            .first { meta.getColumnLabel(it) == columnLabel }
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.StatementResultSet::class.java
    }

}