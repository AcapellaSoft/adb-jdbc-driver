package ru.acapella.db.jdbc

import com.google.protobuf.Empty
import io.grpc.ManagedChannelBuilder
import ru.acapella.db.grpc.LoginRequest
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

    private val urlRegex = Regex("jdbc:acapelladb://([^:]+):(\\d+)(/(.+))?")

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
        val database = urlMatch.groupValues[4].takeIf { it.isNotEmpty() }
        val channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build()
        var txService = TransactionGrpc.newBlockingStub(channel)
        var sqlService = SqlGrpc.newBlockingStub(channel)
        var sqlStreamService = SqlGrpc.newStub(channel)
        val meta = sqlService.metadata(Empty.getDefaultInstance())

        val userName = info["user"] as String?
        val password = info["password"] as String?
        if (userName != null && password != null) {
            val response = sqlService.login(LoginRequest.newBuilder()
                .setUserName(userName)
                .setPassword(password)
                .build())
            val credentials = JwtClientCredentials(response.token)
            txService = txService.withCallCredentials(credentials)
            sqlService = sqlService.withCallCredentials(credentials)
            sqlStreamService = sqlStreamService.withCallCredentials(credentials)
        }

        Connection(txService, sqlService, sqlStreamService, url, database, meta)
    }
}