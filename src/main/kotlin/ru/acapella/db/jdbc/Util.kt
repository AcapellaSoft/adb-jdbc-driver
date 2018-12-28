package ru.acapella.db.jdbc

import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.math.BigDecimal
import java.sql.*

object ErrorCode {
    // common (10xx)

    // transaction (11xx)
    const val TX_CONFLICT = 1101

    // sql (12xx)
    const val SQL_EXCEPTION = 1201
}

private val errorKey: Metadata.Key<Int> = Metadata.Key.of("INTERNAL_CODE", object : Metadata.AsciiMarshaller<Int> {
    override fun toAsciiString(value: Int) = value.toString()
    override fun parseAsciiString(serialized: String) = serialized.toInt()
})

internal fun sqlTypeToJavaClass(type: Int): Class<*> {
    return when (type) {
        Types.BIGINT -> Long::class.java
        Types.BINARY -> ByteArray::class.java
        Types.BOOLEAN -> Boolean::class.java
        Types.DATE -> Date::class.java
        Types.DECIMAL -> BigDecimal::class.java
        Types.DOUBLE -> Double::class.java
        Types.FLOAT -> Double::class.java
        Types.INTEGER -> Int::class.java
        Types.REAL -> Float::class.java
        Types.SMALLINT -> Short::class.java
        Types.TIME -> Time::class.java
        Types.TIMESTAMP -> Timestamp::class.java
        Types.VARBINARY -> ByteArray::class.java
        Types.VARCHAR -> String::class.java
        else -> throw SQLFeatureNotSupportedException("Unsupported parameter type $type")
    }
}

internal fun sqlTypeToJavaClassName(type: Int): String = sqlTypeToJavaClass(type).name

internal fun sqlTypeToName(type: Int) = JDBCType.valueOf(type).name

internal inline fun <T> convertError(block: () -> T): T {
    try {
        return block()
    } catch (ex: StatusRuntimeException) {
        if (ex.status.code == Status.Code.INTERNAL) {
            val internalCode = ex.trailers[errorKey]
            when (internalCode) {
                ErrorCode.TX_CONFLICT -> throw SQLTransactionRollbackException(ex.message)
                else -> throw SQLException(ex.message)
            }
        }
        throw ex
    }
}