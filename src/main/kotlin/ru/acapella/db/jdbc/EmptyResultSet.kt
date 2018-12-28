package ru.acapella.db.jdbc

import java.sql.Types

object EmptyResultSet : ListResultSet(emptyList()) {
    override fun columnType(index: Int) = Types.NULL
    override fun findColumn(columnLabel: String) = -1
    override fun getStatement() = null
    override fun getMetaData() = null
}