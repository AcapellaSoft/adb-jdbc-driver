package ru.acapella.db.jdbc

import ru.acapella.db.grpc.SqlResultSetPb
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.sql.ResultSet
import java.util.*

class ResultSet(
    private val statement: Statement,
    private val meta: ResultSetMetaData,
    private val response: SqlResultSetPb
) : ResultSet {
    private var rowIndex = -1
    private var closed = false

    override fun findColumn(columnLabel: String): Int {
        return (0 until meta.columnCount)
            .indexOfFirst { meta.getColumnLabel(it) == columnLabel }
    }

    override fun getNClob(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getNClob(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateNString(columnIndex: Int, nString: String?) = throw SQLFeatureNotSupportedException()
    override fun updateNString(columnLabel: String?, nString: String?) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) = throw SQLFeatureNotSupportedException()
    override fun updateTimestamp(columnLabel: String?, x: Timestamp?) = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateInt(columnIndex: Int, x: Int) = throw SQLFeatureNotSupportedException()
    override fun updateInt(columnLabel: String?, x: Int) = throw SQLFeatureNotSupportedException()
    override fun moveToInsertRow() = throw SQLFeatureNotSupportedException()
    override fun beforeFirst() = throw SQLFeatureNotSupportedException()
    override fun updateFloat(columnIndex: Int, x: Float) = throw SQLFeatureNotSupportedException()
    override fun updateFloat(columnLabel: String?, x: Float) = throw SQLFeatureNotSupportedException()
    override fun updateBytes(columnIndex: Int, x: ByteArray?) = throw SQLFeatureNotSupportedException()
    override fun updateBytes(columnLabel: String?, x: ByteArray?) = throw SQLFeatureNotSupportedException()
    override fun insertRow() = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun getURL(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getURL(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateShort(columnIndex: Int, x: Short) = throw SQLFeatureNotSupportedException()
    override fun updateShort(columnLabel: String?, x: Short) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnIndex: Int, nClob: NClob?) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnLabel: String?, nClob: NClob?) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateRef(columnIndex: Int, x: Ref?) = throw SQLFeatureNotSupportedException()
    override fun updateRef(columnLabel: String?, x: Ref?) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnIndex: Int, x: Any?) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnLabel: String?, x: Any?) = throw SQLFeatureNotSupportedException()
    override fun afterLast() = throw SQLFeatureNotSupportedException()
    override fun updateLong(columnIndex: Int, x: Long) = throw SQLFeatureNotSupportedException()
    override fun updateLong(columnLabel: String?, x: Long) = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnIndex: Int, x: Clob?)  = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnLabel: String?, x: Clob?) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
    override fun updateDate(columnIndex: Int, x: Date?) = throw SQLFeatureNotSupportedException()
    override fun updateDate(columnLabel: String?, x: Date?) = throw SQLFeatureNotSupportedException()
    override fun previous() = throw SQLFeatureNotSupportedException()
    override fun updateDouble(columnIndex: Int, x: Double) = throw SQLFeatureNotSupportedException()
    override fun updateDouble(columnLabel: String?, x: Double) = throw SQLFeatureNotSupportedException()
    override fun getClob(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getClob(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnIndex: Int, x: Blob?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnLabel: String?, x: Blob?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateByte(columnIndex: Int, x: Byte) = throw SQLFeatureNotSupportedException()
    override fun updateByte(columnLabel: String?, x: Byte) = throw SQLFeatureNotSupportedException()
    override fun updateRow() = throw SQLFeatureNotSupportedException()
    override fun deleteRow() = throw SQLFeatureNotSupportedException()
    override fun getNString(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getNString(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun getCursorName() = throw SQLFeatureNotSupportedException()
    override fun getArray(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getArray(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun cancelRowUpdates() = throw SQLFeatureNotSupportedException()
    override fun updateString(columnIndex: Int, x: String?) = throw SQLFeatureNotSupportedException()
    override fun updateString(columnLabel: String?, x: String?) = throw SQLFeatureNotSupportedException()
    override fun setFetchDirection(direction: Int) = throw SQLFeatureNotSupportedException()
    override fun getCharacterStream(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getCharacterStream(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateBoolean(columnIndex: Int, x: Boolean) = throw SQLFeatureNotSupportedException()
    override fun updateBoolean(columnLabel: String?, x: Boolean) = throw SQLFeatureNotSupportedException()
    override fun refreshRow() = throw SQLFeatureNotSupportedException()
    override fun rowUpdated() = throw SQLFeatureNotSupportedException()
    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
    override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
    override fun getAsciiStream(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getAsciiStream(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateTime(columnIndex: Int, x: Time?) = throw SQLFeatureNotSupportedException()
    override fun updateTime(columnLabel: String?, x: Time?) = throw SQLFeatureNotSupportedException()
    override fun getRef(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getRef(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun moveToCurrentRow() = throw SQLFeatureNotSupportedException()
    override fun updateRowId(columnIndex: Int, x: RowId?) = throw SQLFeatureNotSupportedException()
    override fun updateRowId(columnLabel: String?, x: RowId?) = throw SQLFeatureNotSupportedException()
    override fun getNCharacterStream(columnIndex: Int): Reader = throw SQLFeatureNotSupportedException()
    override fun getNCharacterStream(columnLabel: String?): Reader = throw SQLFeatureNotSupportedException()
    override fun updateArray(columnIndex: Int, x: Array?) = throw SQLFeatureNotSupportedException()
    override fun updateArray(columnLabel: String?, x: Array?) = throw SQLFeatureNotSupportedException()
    override fun getUnicodeStream(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getUnicodeStream(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun rowInserted() = throw SQLFeatureNotSupportedException()
    override fun updateNull(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun updateNull(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnIndex: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun getBigDecimal(columnIndex: Int, scale: Int) = throw SQLFeatureNotSupportedException()
    override fun getBigDecimal(columnLabel: String?, scale: Int) = throw SQLFeatureNotSupportedException()
    override fun last() = throw SQLFeatureNotSupportedException()
    override fun relative(rows: Int) = throw SQLFeatureNotSupportedException()
    override fun absolute(row: Int) = throw SQLFeatureNotSupportedException()
    override fun wasNull() = throw SQLFeatureNotSupportedException()
    override fun getRow() = throw SQLFeatureNotSupportedException()
    override fun first() = throw SQLFeatureNotSupportedException()
    override fun setFetchSize(rows: Int) = throw SQLFeatureNotSupportedException()
    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?) = throw SQLFeatureNotSupportedException()
    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?) = throw SQLFeatureNotSupportedException()

    override fun getStatement() = statement
    override fun getWarnings() = null
    override fun isFirst() = rowIndex == 0
    override fun isLast() = rowIndex == response.rowsCount - 1
    override fun rowDeleted() = false
    override fun isAfterLast() = rowIndex >= response.rowsCount
    override fun getType() = ResultSet.TYPE_FORWARD_ONLY
    override fun getHoldability() = ResultSet.CLOSE_CURSORS_AT_COMMIT
    override fun getFetchSize() = 0
    override fun isClosed() = closed
    override fun isBeforeFirst() = rowIndex == -1
    override fun getConcurrency() = ResultSet.CONCUR_READ_ONLY
    override fun clearWarnings() {}
    override fun getMetaData() = meta
    override fun getFetchDirection() = ResultSet.FETCH_FORWARD

    override fun close() {
        closed = true
    }

    override fun getDate(columnIndex: Int): Date? = getColumn(columnIndex)
    override fun getDate(columnLabel: String): Date? = getColumn(columnLabel)
    override fun getDate(columnIndex: Int, cal: Calendar?) = getDate(columnIndex)
    override fun getDate(columnLabel: String, cal: Calendar?) = getDate(columnLabel)
    override fun getBoolean(columnIndex: Int): Boolean = getColumn(columnIndex) ?: false
    override fun getBoolean(columnLabel: String): Boolean = getColumn(columnLabel) ?: false
    override fun getBigDecimal(columnIndex: Int): BigDecimal? = getColumn(columnIndex)
    override fun getBigDecimal(columnLabel: String): BigDecimal? = getColumn(columnLabel)
    override fun getTime(columnIndex: Int): Time? = getColumn(columnIndex)
    override fun getTime(columnLabel: String): Time? = getColumn(columnLabel)
    override fun getTime(columnIndex: Int, cal: Calendar?) = getTime(columnIndex)
    override fun getTime(columnLabel: String, cal: Calendar?) = getTime(columnLabel)
    override fun getFloat(columnIndex: Int): Float = getColumn(columnIndex) ?: 0.0f
    override fun getFloat(columnLabel: String): Float = getColumn(columnLabel) ?: 0.0f
    override fun getByte(columnIndex: Int): Byte = getColumn(columnIndex) ?: 0.toByte()
    override fun getByte(columnLabel: String): Byte = getColumn(columnLabel) ?: 0.toByte()
    override fun getString(columnIndex: Int): String? = getColumn(columnIndex)
    override fun getString(columnLabel: String): String?  = getColumn(columnLabel)
    override fun getObject(columnIndex: Int): Any? = getColumn(columnIndex)
    override fun getObject(columnLabel: String): Any? = getColumn(columnLabel)
    override fun getLong(columnIndex: Int): Long = getColumn(columnIndex) ?: 0L
    override fun getLong(columnLabel: String): Long = getColumn(columnLabel) ?: 0L
    override fun getShort(columnIndex: Int): Short = getColumn(columnIndex) ?: 0.toShort()
    override fun getShort(columnLabel: String): Short = getColumn(columnLabel) ?: 0.toShort()
    override fun getTimestamp(columnIndex: Int): Timestamp? = getColumn(columnIndex)
    override fun getTimestamp(columnLabel: String): Timestamp? = getColumn(columnLabel)
    override fun getTimestamp(columnIndex: Int, cal: Calendar?) = getTimestamp(columnIndex)
    override fun getTimestamp(columnLabel: String, cal: Calendar?) = getTimestamp(columnLabel)
    override fun getBytes(columnIndex: Int): ByteArray? = getColumn(columnIndex)
    override fun getBytes(columnLabel: String): ByteArray? = getColumn(columnLabel)
    override fun getDouble(columnIndex: Int): Double = getColumn(columnIndex) ?: 0.0
    override fun getDouble(columnLabel: String): Double = getColumn(columnLabel) ?: 0.0
    override fun getInt(columnIndex: Int): Int = getColumn(columnIndex) ?: 0
    override fun getInt(columnLabel: String): Int = getColumn(columnLabel) ?: 0

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>): T {
        val sqlType = meta.getColumnType(columnIndex)
        val value: Any? = getColumn(columnIndex)
        if (value != null && !type.isInstance(value)) throw SQLException("Cannot convert column type $sqlType to $type")
        return value as T
    }

    override fun <T : Any?> getObject(columnLabel: String, type: Class<T>): T {
        val index = findColumn(columnLabel)
        return getObject(index, type)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> unwrap(iface: Class<T>): T {
        if (isWrapperFor(iface)) return this as T
        throw SQLException()
    }

    override fun next(): Boolean {
        if (rowIndex < response.rowsCount) {
            rowIndex += 1
        }
        return !isAfterLast
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.ResultSet::class.java
    }

    private inline fun <reified T> getColumn(columnIndex: Int): T {
        val row = response.rowsList[rowIndex]
        val variant = row.fieldsList[columnIndex - 1]
        val value = when {
            variant.hasVDecimal() -> BigDecimal(variant.vDecimal)
            variant.hasVBytes() -> variant.vBytes.toByteArray()
            variant.hasVByte() -> variant.vByte.toByte()
            variant.hasVString() -> variant.vString
            variant.hasVDouble() -> variant.vDouble
            variant.hasVFloat() -> variant.vFloat
            variant.hasVLong() -> variant.vLong
            variant.hasVInt() -> variant.vInt
            variant.hasVBoolean() -> variant.vBoolean
            variant.hasVDate() -> Date(variant.vDate)
            variant.hasVShort() -> variant.vShort.toShort()
            variant.hasVTime() -> Time(variant.vTime)
            variant.hasVTimestamp() -> Timestamp(variant.vTimestamp)
            else -> null
        }
        return value as T
    }

    private inline fun <reified T> getColumn(columnLabel: String): T {
        return getColumn(findColumn(columnLabel))
    }
}