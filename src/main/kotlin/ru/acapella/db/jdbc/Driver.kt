package ru.acapella.db.jdbc

import com.google.protobuf.Empty
import io.grpc.ManagedChannelBuilder
import ru.acapella.db.grpc.SqlGrpc
import ru.acapella.db.grpc.TransactionGrpc
import java.sql.Driver
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.*

private val pack = Package.getPackage("ru.acapella.db.jdbc")
internal const val DB_DRIVER_NAME = "Acapella JDBC Driver"
internal val DB_DRIVER_VERSION = pack.implementationVersion ?: "0.0.0"
internal val DB_DRIVER_MAJOR_VERSION = pack.implementationVersion?.split(".")?.getOrNull(0)?.toInt() ?: 0
internal val DB_DRIVER_MINOR_VERSION = pack.implementationVersion?.split(".")?.getOrNull(1)?.toInt() ?: 0

@Suppress("unused")
class Driver : Driver {
    companion object {
        init {
            DriverManager.registerDriver(Driver())
        }
    }

    private val urlRegex = Regex("jdbc:acapelladb://([^:]+):(\\d+)")

    override fun getMinorVersion() = DB_DRIVER_MINOR_VERSION
    override fun getMajorVersion() = DB_DRIVER_MAJOR_VERSION
    override fun getParentLogger() = throw SQLFeatureNotSupportedException()
    override fun getPropertyInfo(url: String, info: Properties) = TODO()
    override fun jdbcCompliant() = true

    override fun acceptsURL(url: String): Boolean {
        return urlRegex.matchEntire(url) != null
    }

    override fun connect(url: String, info: Properties): Connection = convertError {
        val urlMatch = urlRegex.matchEntire(url) ?: throw SQLException("Bad connection url '$url'")
        val host = urlMatch.groupValues[1]
        val port = urlMatch.groupValues[2].toInt()
        val channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build()
        val txService = TransactionGrpc.newBlockingStub(channel)
        val sqlService = SqlGrpc.newBlockingStub(channel)
        val meta = sqlService.metadata(Empty.getDefaultInstance())
        Connection(txService, sqlService, url, meta)
    }
}