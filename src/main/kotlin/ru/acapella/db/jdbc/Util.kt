package ru.acapella.db.jdbc

import java.math.BigDecimal
import java.sql.*

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