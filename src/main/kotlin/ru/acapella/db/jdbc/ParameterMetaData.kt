package ru.acapella.db.jdbc

import ru.acapella.db.grpc.SqlParameterMetaPb
import java.sql.ParameterMetaData
import java.sql.SQLException

class ParameterMetaData(private val parameters: List<SqlParameterMetaPb>) : ParameterMetaData {
    override fun getParameterClassName(param: Int): String {
        val type = getParameterType(param)
        return sqlTypeToJavaClassName(type)
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.ParameterMetaData::class.java
    }

    override fun getParameterTypeName(param: Int): String {
        val type = getParameterType(param)
        return sqlTypeToName(type)
    }

    override fun isNullable(param: Int) = ParameterMetaData.parameterNullable

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> unwrap(iface: Class<T>): T {
        if (isWrapperFor(iface)) return this as T
        throw SQLException()
    }

    override fun getParameterMode(param: Int) = ParameterMetaData.parameterModeIn
    override fun isSigned(param: Int) = true
    override fun getPrecision(param: Int) = 0
    override fun getParameterCount() = parameters.size
    override fun getScale(param: Int) = 0
    override fun getParameterType(param: Int) = parameters[param - 1].type
}