package ru.acapella.db.jdbc

import io.grpc.*
import java.util.concurrent.Executor

val METADATA_JWT_KEY: Metadata.Key<String> = Metadata.Key.of("jwt", Metadata.ASCII_STRING_MARSHALLER)

class JwtClientCredentials(private val jwt: String) : CallCredentials {
    override fun applyRequestMetadata(
        method: MethodDescriptor<*, *>,
        attrs: Attributes,
        appExecutor: Executor,
        applier: CallCredentials.MetadataApplier
    ) {
        appExecutor.execute {
            try {
                val headers = Metadata()
                headers.put(METADATA_JWT_KEY, jwt)
                applier.apply(headers)
            } catch (ex: Throwable) {
                applier.fail(Status.UNAUTHENTICATED.withCause(ex))
            }
        }
    }

    override fun thisUsesUnstableApi() {}
}