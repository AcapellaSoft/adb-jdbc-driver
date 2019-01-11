package ru.acapella.db.jdbc

import com.google.protobuf.ByteString
import ru.acapella.db.grpc.SqlExecuteRequestPb
import ru.acapella.db.grpc.SqlQueryRequestPb
import ru.acapella.db.grpc.SqlVariantPb
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Date
import java.sql.PreparedStatement
import java.util.*

class PreparedStatement(
    connection: Connection,
    private val sql: String,
    private val parameterMeta: ParameterMetaData,
    private val resultMeta: ResultSetMetaData
) : PreparedStatement, Statement(connection) {
    private val parameters = Array<SqlVariantPb?>(parameterMeta.parameterCount) { null }

    override fun setRef(parameterIndex: Int, x: Ref?) = throw SQLFeatureNotSupportedException()
    override fun setBlob(parameterIndex: Int, x: Blob?) = throw SQLFeatureNotSupportedException()
    override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setBlob(parameterIndex: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setCharacterStream(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun setArray(parameterIndex: Int, x: java.sql.Array?) = throw SQLFeatureNotSupportedException()
    override fun setClob(parameterIndex: Int, x: Clob?) = throw SQLFeatureNotSupportedException()
    override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setClob(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun setNString(parameterIndex: Int, value: String?) = throw SQLFeatureNotSupportedException()
    override fun setURL(parameterIndex: Int, x: URL?) = throw SQLFeatureNotSupportedException()
    override fun setRowId(parameterIndex: Int, x: RowId?) = throw SQLFeatureNotSupportedException()
    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setBinaryStream(parameterIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setNCharacterStream(parameterIndex: Int, value: Reader?) = throw SQLFeatureNotSupportedException()
    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int)  = throw SQLFeatureNotSupportedException()
    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setAsciiStream(parameterIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun setNClob(parameterIndex: Int, value: NClob?) = throw SQLFeatureNotSupportedException()
    override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setNClob(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) = throw SQLFeatureNotSupportedException()
    override fun addBatch() = throw SQLFeatureNotSupportedException()

    override fun getParameterMetaData() = parameterMeta
    override fun getMetaData() = resultMeta

    override fun clearParameters() {
        for (i in parameters.indices)
            parameters[i] = null
    }

    override fun setObject(parameterIndex: Int, x: Any?) {
        val type = parameterMeta.getParameterType(parameterIndex)
        when (type) {
            Types.BIGINT -> setLong(parameterIndex, x as Long? ?: 0L)
            Types.BINARY -> setBytes(parameterIndex, x as ByteArray?)
            Types.BOOLEAN -> setBoolean(parameterIndex, x as Boolean? ?: false)
            Types.DATE -> setDate(parameterIndex, x as Date?)
            Types.DECIMAL -> setBigDecimal(parameterIndex, x as BigDecimal?)
            Types.DOUBLE -> setDouble(parameterIndex, x as Double? ?: 0.0)
            Types.FLOAT -> setDouble(parameterIndex, x as Double? ?: 0.0)
            Types.INTEGER -> setInt(parameterIndex, x as Int? ?: 0)
            Types.REAL -> setFloat(parameterIndex, x as Float? ?: 0.0f)
            Types.SMALLINT -> setShort(parameterIndex, x as Short? ?: 0)
            Types.TIME -> setTime(parameterIndex, x as Time?)
            Types.TIMESTAMP -> setTimestamp(parameterIndex, x as Timestamp?)
            Types.VARBINARY -> setBytes(parameterIndex, x as ByteArray?)
            Types.VARCHAR -> setString(parameterIndex, x as String?)
            else -> throw SQLFeatureNotSupportedException("Unsupported parameter type $type")
        }
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) {
        val type = parameterMeta.getParameterType(parameterIndex)
        if (type != targetSqlType) throw SQLException("targetSqlType ($targetSqlType) != parameterType ($type)")
        setObject(parameterIndex, x)
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
        setObject(parameterIndex, x, targetSqlType, 0)
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        setParameter(parameterIndex, x) { vBytes = ByteString.copyFrom(it) }
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        setParameter(parameterIndex, x) { vLong = it }
    }

    override fun setFloat(parameterIndex: Int, x: Float) {
        setParameter(parameterIndex, x) { vFloat = it }
    }

    override fun setTime(parameterIndex: Int, x: Time?) {
        setParameter(parameterIndex, x) { vTime = it.time }
    }

    override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) {
        setTime(parameterIndex, x)
    }

    override fun setDate(parameterIndex: Int, x: Date?) {
        setParameter(parameterIndex, x) { vDate = it.time }
    }

    override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) {
        setDate(parameterIndex, x)
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        setParameter(parameterIndex, x) { vInt = it }
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        setParameter(parameterIndex, x) { vDouble = it }
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        setParameter(parameterIndex, x) { vDecimal = it.toString() }
    }

    override fun setString(parameterIndex: Int, x: String?) {
        setParameter(parameterIndex, x) { vString = it }
    }

    override fun setNull(parameterIndex: Int, sqlType: Int) {
        val type = parameterMeta.getParameterType(parameterIndex)
        if (type != sqlType) throw SQLException("sqlType ($sqlType) != parameterType ($type)")
        setParameter(parameterIndex, null) { }
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        setParameter(parameterIndex, x) { vTimestamp = it.time }
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) {
        setTimestamp(parameterIndex, x)
    }

    override fun setShort(parameterIndex: Int, x: Short) {
        setParameter(parameterIndex, x) { vShort = it.toInt() }
    }

    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        setParameter(parameterIndex, x) { vBoolean = it }
    }

    override fun setByte(parameterIndex: Int, x: Byte) {
        setParameter(parameterIndex, x) { vByte = it.toInt() }
    }

    override fun executeQuery() = convertError {
        // todo partial fetch
        connection.withTransaction { tx ->
            val requestBuilder = SqlQueryRequestPb.newBuilder()
                .setSql(sql)
                .addAllParameters(parameters.asList())
            if (tx != null) requestBuilder.transaction = tx
            connection.database?.let { requestBuilder.database = it }
            if (fetchSize != 0) requestBuilder.fetchSize = fetchSize
            val response = connection.sqlService.query(requestBuilder.build())
            StatementResultSet(response, this, resultMeta)
        }
    }

    override fun executeUpdate(): Int {
        return executeLargeUpdate().toInt()
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> unwrap(iface: Class<T>): T {
        if (isWrapperFor(iface)) return this as T
        throw SQLException()
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.PreparedStatement::class.java
    }

    override fun execute(): Boolean {
        executeLargeUpdate()
        return false
    }

    override fun executeLargeUpdate(): Long = convertError {
        connection.withTransaction { tx ->
            val requestBuilder = SqlExecuteRequestPb.newBuilder()
                .setSql(sql)
                .addAllParameters(parameters.asList())
            if (tx != null) requestBuilder.transaction = tx
            connection.database?.let { requestBuilder.database = it }
            val response = connection.sqlService.execute(requestBuilder.build())
            response.rowsCount
        }
    }

    private inline fun <T : Any> setParameter(index: Int, x: T?, applyValue: SqlVariantPb.Builder.(T) -> Unit) {
        val builder = SqlVariantPb.newBuilder()
        if (x != null) {
            builder.applyValue(x)
        }
        val variant = builder.build()
        parameters[index - 1] = variant
    }
}