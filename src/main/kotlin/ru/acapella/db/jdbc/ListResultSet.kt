package ru.acapella.db.jdbc

import ru.acapella.db.grpc.SqlVariantPb

abstract class ListResultSet(
    private val rows: List<List<SqlVariantPb>>,
    fetchSize: Int
) : VariantResultSet(fetchSize) {
    private var rowIndex = -1

    override fun isFirst() = rowIndex == 0
    override fun isLast() = rowIndex == rows.size - 1
    override fun isAfterLast() = rowIndex >= rows.size
    override fun isBeforeFirst() = rowIndex == -1
    override val row: List<SqlVariantPb> get() = rows[rowIndex]

    override fun next(): Boolean {
        if (rowIndex < rows.size) {
            rowIndex += 1
        }
        return !isAfterLast
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.StatementResultSet::class.java
    }
}