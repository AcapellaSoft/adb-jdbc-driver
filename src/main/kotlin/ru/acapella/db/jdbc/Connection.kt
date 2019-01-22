package ru.acapella.db.jdbc

import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import ru.acapella.db.grpc.*
import java.sql.*
import java.sql.Connection
import java.sql.Statement
import java.util.*
import java.util.concurrent.Executor

class Connection(
    internal val txService: TransactionGrpc.TransactionBlockingStub,
    internal val sqlService: SqlGrpc.SqlBlockingStub,
    internal val sqlStreamService: SqlGrpc.SqlStub,
    internal val url: String,
    internal val database: String?,
    meta: SqlDatabaseMetadataPb
) : Connection {
    private var autoCommit = false
    private var closed = false
    private var transaction: ByteString? = null
    private val metadata = DatabaseMetaData(this, meta)

    override fun prepareStatement(sql: String?, columnIndexes: IntArray?) = throw SQLFeatureNotSupportedException()
    override fun prepareStatement(sql: String?, columnNames: Array<out String>?) = throw SQLFeatureNotSupportedException()
    override fun rollback(savepoint: Savepoint?) = throw SQLFeatureNotSupportedException()
    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) = throw SQLFeatureNotSupportedException()
    override fun setTransactionIsolation(level: Int) = throw SQLFeatureNotSupportedException()
    override fun prepareCall(sql: String?) = throw SQLFeatureNotSupportedException()
    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int) = throw SQLFeatureNotSupportedException()
    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int) = throw SQLFeatureNotSupportedException()
    override fun getClientInfo(name: String?) = throw SQLFeatureNotSupportedException()
    override fun getClientInfo() = throw SQLFeatureNotSupportedException()
    override fun setCatalog(catalog: String?) = throw SQLFeatureNotSupportedException()
    override fun setHoldability(holdability: Int) = throw SQLFeatureNotSupportedException()
    override fun isValid(timeout: Int) = throw SQLFeatureNotSupportedException()
    override fun createNClob() = throw SQLFeatureNotSupportedException()
    override fun createBlob() = throw SQLFeatureNotSupportedException()
    override fun createArrayOf(typeName: String?, elements: Array<out Any>?) = throw SQLFeatureNotSupportedException()
    override fun setReadOnly(readOnly: Boolean) = throw SQLFeatureNotSupportedException()
    override fun nativeSQL(sql: String?) = throw SQLFeatureNotSupportedException()
    override fun createStruct(typeName: String?, attributes: Array<out Any>?) = throw SQLFeatureNotSupportedException()
    override fun setClientInfo(name: String?, value: String?) = throw SQLFeatureNotSupportedException()
    override fun setClientInfo(properties: Properties?) = throw SQLFeatureNotSupportedException()
    override fun releaseSavepoint(savepoint: Savepoint?) = throw SQLFeatureNotSupportedException()
    override fun createClob() = throw SQLFeatureNotSupportedException()
    override fun isReadOnly() = throw SQLFeatureNotSupportedException()
    override fun setSavepoint() = throw SQLFeatureNotSupportedException()
    override fun setSavepoint(name: String?) = throw SQLFeatureNotSupportedException()
    override fun getTypeMap() = throw SQLFeatureNotSupportedException()
    override fun setSchema(schema: String?) = throw SQLFeatureNotSupportedException()
    override fun getNetworkTimeout() = throw SQLFeatureNotSupportedException()
    override fun setTypeMap(map: MutableMap<String, Class<*>>?) = throw SQLFeatureNotSupportedException()
    override fun createSQLXML() = throw SQLFeatureNotSupportedException()

    override fun getHoldability() = ResultSet.CLOSE_CURSORS_AT_COMMIT
    override fun abort(executor: Executor?) {}
    override fun getAutoCommit() = autoCommit
    override fun getWarnings() = null
    override fun isClosed() = closed
    override fun clearWarnings() {}
    override fun getTransactionIsolation() = Connection.TRANSACTION_SERIALIZABLE
    override fun getMetaData() = metadata
    override fun getCatalog() = null
    override fun getSchema() = database

    override fun prepareStatement(sql: String): PreparedStatement {
        return prepareStatement(
            sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT
        )
    }

    override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        return prepareStatement(
            sql,
            resultSetType,
            resultSetConcurrency,
            ResultSet.CLOSE_CURSORS_AT_COMMIT
        )
    }

    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement = convertError {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) throw SQLFeatureNotSupportedException()
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) throw SQLFeatureNotSupportedException()
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) throw SQLFeatureNotSupportedException()

        val request = SqlPrepareRequestPb.newBuilder()
            .setSql(sql)
        if (database != null) request.database = database

        val response = sqlService.prepare(request.build())

        val parameterMeta = ParameterMetaData(response.parametersList)
        val resultMeta = ResultSetMetaData(response.columnsList)
        PreparedStatement(this, sql, parameterMeta, resultMeta)
    }

    override fun prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement {
        if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) throw SQLFeatureNotSupportedException()
        return prepareStatement(sql)
    }

    override fun rollback(): Unit = convertError {
        val id = transaction ?: return
        try {
            txService.rollback(RollbackTransactionRequestPb.newBuilder()
                .setId(id)
                .build())
        } finally {
            transaction = null
        }
    }

    override fun commit(): Unit = convertError {
        val id = transaction ?: return
        try {
            txService.commit(CommitTransactionRequestPb.newBuilder()
                .setId(id)
                .build())
        } finally {
            transaction = null
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> unwrap(iface: Class<T>): T {
        if (isWrapperFor(iface)) return this as T
        throw SQLException()
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        if (this.autoCommit != autoCommit && transaction != null) {
            rollback()
        }
        this.autoCommit = autoCommit
    }

    override fun close() {
        closed = true
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.Connection::class.java
    }

    override fun createStatement(): Statement {
        return createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT
        )
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT)
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) throw SQLFeatureNotSupportedException()
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) throw SQLFeatureNotSupportedException()
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) throw SQLFeatureNotSupportedException()

        return Statement(this)
    }

    internal fun <T> withTransaction(block: (transaction: ByteString?) -> T): T {
        if (transaction == null && !autoCommit) {
            transaction = convertError {
                txService.begin(Empty.getDefaultInstance()).id
            }
        }
        return block(transaction)
    }
}