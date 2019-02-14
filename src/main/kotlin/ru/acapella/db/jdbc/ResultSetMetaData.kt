package ru.acapella.db.jdbc

import ru.acapella.db.grpc.SqlColumnMetaPb
import ru.acapella.db.grpc.SqlRowPb
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types
import kotlin.math.max

class ResultSetMetaData(
    private val columns: List<SqlColumnMetaPb>,
    private val rows: List<SqlRowPb>?
) : ResultSetMetaData {
    private val displaySizes = columns.mapTo(ArrayList()) { it.displaySize }

    override fun getCatalogName(column: Int) = throw SQLFeatureNotSupportedException()

    override fun getTableName(column: Int): String = columns[column - 1].table
    override fun isNullable(column: Int): Int = ResultSetMetaData.columnNullable
    override fun isDefinitelyWritable(column: Int) = false
    override fun isSearchable(column: Int) = false
    override fun getPrecision(column: Int) = 0
    override fun isCaseSensitive(column: Int) = true
    override fun getScale(column: Int) = 0
    override fun getSchemaName(column: Int): String = columns[column - 1].schema
    override fun getColumnType(column: Int) = columns[column - 1].type
    override fun isCurrency(column: Int) = false
    override fun getColumnLabel(column: Int): String = columns[column - 1].label
    override fun isWritable(column: Int) = false
    override fun isReadOnly(column: Int) = true
    override fun isSigned(column: Int) = true
    override fun getColumnName(column: Int): String = columns[column - 1].name
    override fun isAutoIncrement(column: Int) = false
    override fun getColumnCount() = columns.size

    override fun getColumnDisplaySize(column: Int): Int {
        var size = displaySizes[column - 1]
        val type = columns[column - 1].type
        // fixme better display size
        if (size < 0 && (type == Types.VARCHAR || type == Types.CHAR) && rows != null) {
            size = 0
            for (row in rows) {
                size = max(size, row.getFields(column - 1).vString.length)
            }
            displaySizes[column - 1] = size
        }
        return size
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> unwrap(iface: Class<T>): T {
        if (isWrapperFor(iface)) return this as T
        throw SQLException()
    }

    override fun getColumnClassName(column: Int): String {
        val type = getColumnType(column)
        return sqlTypeToJavaClassName(type)
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.ResultSetMetaData::class.java
    }

    override fun getColumnTypeName(column: Int): String {
        val type = getColumnType(column)
        return sqlTypeToName(type)
    }
}