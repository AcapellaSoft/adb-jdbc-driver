package ru.acapella.db.jdbc

import ru.acapella.db.grpc.SqlColumnsMetaRequestPb
import ru.acapella.db.grpc.SqlDatabaseMetadataPb
import ru.acapella.db.grpc.SqlPrimaryKeysMetaRequestPb
import ru.acapella.db.grpc.SqlTablesMetaRequestPb
import java.sql.*
import java.sql.Connection
import java.sql.DatabaseMetaData

class DatabaseMetaData(
    private val connection: ru.acapella.db.jdbc.Connection,
    private val meta: SqlDatabaseMetadataPb
) : DatabaseMetaData {
    private val databaseVersionParts = meta.databaseProductVersion
        .split(".")
        .take(2)
        .map { it.toInt() }

    override fun getCatalogTerm() = "catalog"
    override fun supportsSubqueriesInQuantifieds() = true
    override fun supportsGetGeneratedKeys() = false
    override fun supportsCoreSQLGrammar() = true
    override fun getMaxColumnsInIndex() = 0
    override fun insertsAreDetected(type: Int) = false
    override fun supportsIntegrityEnhancementFacility() = false
    override fun getConnection() = connection
    override fun supportsOpenStatementsAcrossRollback() = true
    override fun getMaxProcedureNameLength() = 0
    override fun supportsCatalogsInDataManipulation() = false
    override fun getMaxUserNameLength() = 0
    override fun supportsStoredFunctionsUsingCallSyntax() = false
    override fun autoCommitFailureClosesAllResultSets() = false
    override fun getMaxColumnsInSelect() = 0
    override fun storesLowerCaseQuotedIdentifiers() = false
    override fun supportsDataDefinitionAndDataManipulationTransactions() = false
    override fun supportsCatalogsInTableDefinitions() = false
    override fun getMaxColumnsInOrderBy() = 0
    override fun storesUpperCaseIdentifiers() = false
    override fun nullsAreSortedLow() = true
    override fun supportsSchemasInIndexDefinitions() = true
    override fun getMaxStatementLength() = 0
    override fun supportsTransactions() = true
    override fun isReadOnly() = false
    override fun usesLocalFiles() = false
    override fun getMaxConnections() = 0
    override fun supportsMultipleResultSets() = false
    override fun dataDefinitionIgnoredInTransactions() = false
    override fun supportsGroupBy() = true
    override fun getMaxTableNameLength() = 0
    override fun dataDefinitionCausesTransactionCommit() = false
    override fun supportsOpenStatementsAcrossCommit() = true
    override fun ownInsertsAreVisible(type: Int) = true
    override fun isCatalogAtStart() = false
    override fun nullsAreSortedAtStart() = false
    override fun supportsANSI92IntermediateSQL() = true
    override fun supportsOuterJoins() = true
    override fun supportsLikeEscapeClause() = true
    override fun supportsPositionedUpdate() = false
    override fun supportsMixedCaseIdentifiers() = true
    override fun supportsLimitedOuterJoins() = true
    override fun getSQLStateType() = DatabaseMetaData.sqlStateSQL
    override fun getMaxRowSize() = 0
    override fun supportsOpenCursorsAcrossRollback() = false
    override fun getMaxTablesInSelect() = 0
    override fun nullsAreSortedHigh() = false
    override fun getURL() = connection.url
    override fun supportsNamedParameters() = false
    override fun supportsConvert() = false
    override fun supportsConvert(fromType: Int, toType: Int) = false
    override fun getMaxStatements() = 0
    override fun allTablesAreSelectable() = true
    override fun supportsMultipleOpenResults() = false
    override fun deletesAreDetected(type: Int) = false
    override fun supportsMinimumSQLGrammar() = true
    override fun getExtraNameCharacters() = ""
    override fun getMaxCursorNameLength() = 0
    override fun nullsAreSortedAtEnd() = false
    override fun supportsSchemasInDataManipulation() = true
    override fun supportsCorrelatedSubqueries() = true
    override fun getDefaultTransactionIsolation() = Connection.TRANSACTION_SERIALIZABLE
    override fun locatorsUpdateCopy() = false
    override fun ownDeletesAreVisible(type: Int) = true
    override fun othersUpdatesAreVisible(type: Int) = false
    override fun supportsStatementPooling() = false
    override fun storesLowerCaseIdentifiers() = false
    override fun supportsCatalogsInIndexDefinitions() = false
    override fun ownUpdatesAreVisible(type: Int) = true
    override fun getMaxColumnsInTable() = 0
    override fun supportsColumnAliasing() = true
    override fun supportsSchemasInProcedureCalls() = false
    override fun usesLocalFilePerTable() = false
    override fun getIdentifierQuoteString() = "'"
    override fun supportsFullOuterJoins() = true
    override fun supportsOrderByUnrelated() = true
    override fun supportsSchemasInTableDefinitions() = true
    override fun supportsCatalogsInProcedureCalls() = false
    override fun getUserName() = null
    override fun supportsTableCorrelationNames() = false
    override fun getMaxIndexLength() = 0
    override fun supportsSubqueriesInExists() = false
    override fun getMaxSchemaNameLength() = 0
    override fun supportsANSI92EntryLevelSQL() = true
    override fun supportsMixedCaseQuotedIdentifiers() = true
    override fun supportsANSI92FullSQL() = true
    override fun supportsAlterTableWithAddColumn() = false
    override fun supportsUnionAll() = true
    override fun getRowIdLifetime() = RowIdLifetime.ROWID_UNSUPPORTED
    override fun doesMaxRowSizeIncludeBlobs() = true
    override fun supportsGroupByUnrelated() = true
    override fun supportsSubqueriesInIns() = true
    override fun supportsStoredProcedures() = false
    override fun supportsPositionedDelete() = false
    override fun supportsAlterTableWithDropColumn() = false
    override fun supportsExpressionsInOrderBy() = true
    override fun getMaxCatalogNameLength() = 0
    override fun supportsExtendedSQLGrammar() = true
    override fun othersInsertsAreVisible(type: Int) = false
    override fun updatesAreDetected(type: Int) = false
    override fun supportsDataManipulationTransactionsOnly() = false
    override fun supportsSubqueriesInComparisons() = true
    override fun supportsSavepoints() = false
    override fun getMaxColumnNameLength() = 0
    override fun nullPlusNonNullIsNull() = true
    override fun supportsGroupByBeyondSelect() = true
    override fun supportsCatalogsInPrivilegeDefinitions() = true
    override fun allProceduresAreCallable() = true
    override fun generatedKeyAlwaysReturned() = false
    override fun storesUpperCaseQuotedIdentifiers() = false
    override fun getMaxCharLiteralLength() = 0
    override fun othersDeletesAreVisible(type: Int) = false
    override fun supportsNonNullableColumns() = false
    override fun supportsUnion() = true
    override fun supportsDifferentTableCorrelationNames() = true
    override fun supportsSchemasInPrivilegeDefinitions() = true
    override fun supportsSelectForUpdate() = false
    override fun supportsMultipleTransactions() = true
    override fun storesMixedCaseQuotedIdentifiers() = true
    override fun supportsOpenCursorsAcrossCommit() = false
    override fun storesMixedCaseIdentifiers() = true
    override fun supportsBatchUpdates() = true // todo
    override fun getResultSetHoldability() = ResultSet.CLOSE_CURSORS_AT_COMMIT
    override fun getSearchStringEscape() = "\\"
    override fun getSchemaTerm() = "schema"
    override fun getProcedureTerm() = "procedure"
    override fun getMaxColumnsInGroupBy() = 0
    override fun getMaxBinaryLiteralLength() = 0
    override fun getTimeDateFunctions() = ""
    override fun getCatalogs() = EmptyResultSet
    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?) = EmptyResultSet
    override fun getFunctionColumns(catalog: String?, schemaPattern: String?, functionNamePattern: String?, columnNamePattern: String?) = EmptyResultSet
    override fun getAttributes(catalog: String?, schemaPattern: String?, typeNamePattern: String?, attributeNamePattern: String?) = EmptyResultSet
    override fun getSystemFunctions() = ""
    override fun getProcedureColumns(catalog: String?, schemaPattern: String?, procedureNamePattern: String?, columnNamePattern: String?) = EmptyResultSet
    override fun getCatalogSeparator() = "."
    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?) = EmptyResultSet
    override fun getTypeInfo() = EmptyResultSet
    override fun getVersionColumns(catalog: String?, schema: String?, table: String?) = EmptyResultSet
    override fun getNumericFunctions() = ""
    override fun getCrossReference(parentCatalog: String?, parentSchema: String?, parentTable: String?, foreignCatalog: String?, foreignSchema: String?, foreignTable: String?) = EmptyResultSet
    override fun getUDTs(catalog: String?, schemaPattern: String?, typeNamePattern: String?, types: IntArray?) = EmptyResultSet
    override fun getStringFunctions() = ""
    override fun getClientInfoProperties() = EmptyResultSet
    override fun getBestRowIdentifier(catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean) = EmptyResultSet
    override fun getPseudoColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?) = EmptyResultSet
    override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?) = EmptyResultSet
    override fun getColumnPrivileges(catalog: String?, schema: String?, table: String?, columnNamePattern: String?) = EmptyResultSet
    override fun getImportedKeys(catalog: String?, schema: String?, table: String?) = EmptyResultSet
    override fun getExportedKeys(catalog: String?, schema: String?, table: String?) = EmptyResultSet
    override fun getSQLKeywords() = ""
    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?) = EmptyResultSet
    override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?) = EmptyResultSet
    override fun getDatabaseProductVersion(): String = meta.databaseProductVersion
    override fun getDatabaseProductName(): String = meta.databaseProductName
    override fun getJDBCMajorVersion() = meta.jdbcMajorVersion
    override fun getJDBCMinorVersion() = meta.jdbcMinorVersion
    override fun getDatabaseMajorVersion() = databaseVersionParts[0]
    override fun getDatabaseMinorVersion() = databaseVersionParts[1]
    override fun getDriverVersion() = DB_DRIVER_VERSION
    override fun getDriverMajorVersion() = DB_DRIVER_MAJOR_VERSION
    override fun getDriverMinorVersion() = DB_DRIVER_MINOR_VERSION
    override fun getDriverName() = DB_DRIVER_NAME

    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY
    }

    override fun supportsResultSetType(type: Int): Boolean {
        return type == ResultSet.TYPE_FORWARD_ONLY
    }

    override fun supportsTransactionIsolationLevel(level: Int): Boolean {
        return level == Connection.TRANSACTION_SERIALIZABLE
    }

    override fun supportsResultSetHoldability(holdability: Int): Boolean {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT
    }

    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet = convertError {
        val requestBuilder = SqlTablesMetaRequestPb.newBuilder()
        if (catalog != null) requestBuilder.catalog = catalog
        if (schemaPattern != null) requestBuilder.schemaPattern = schemaPattern
        if (tableNamePattern != null) requestBuilder.tableNamePattern = tableNamePattern
        if (types != null) requestBuilder.addAllTypes(types.asList())

        val response = connection.sqlService.tablesMetadata(requestBuilder.build())
        StatementResultSet(response, meta = ResultSetMetaData(response.columnsList, response.rowsList))
    }

    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet = convertError {
        val requestBuilder = SqlPrimaryKeysMetaRequestPb.newBuilder()
        if (catalog != null) requestBuilder.catalog = catalog
        if (schema != null) requestBuilder.schema = schema
        if (table != null) requestBuilder.table = table

        val response = connection.sqlService.primaryKeysMetadata(requestBuilder.build())
        StatementResultSet(response, meta = ResultSetMetaData(response.columnsList, response.rowsList))
    }

    override fun getTableTypes(): ResultSet {
        TODO("not implemented")
    }

    override fun getSchemas(): ResultSet {
        TODO("not implemented")
    }

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        TODO("not implemented")
    }

    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet = convertError {
        val requestBuilder = SqlColumnsMetaRequestPb.newBuilder()
        if (catalog != null) requestBuilder.catalog = catalog
        if (schemaPattern != null) requestBuilder.schemaPattern = schemaPattern
        if (tableNamePattern != null) requestBuilder.tableNamePattern = tableNamePattern
        if (columnNamePattern != null) requestBuilder.columnNamePattern = columnNamePattern

        val response = connection.sqlService.columnsMetadata(requestBuilder.build())
        StatementResultSet(response, meta = ResultSetMetaData(response.columnsList, response.rowsList))
    }

    override fun getIndexInfo(catalog: String?, schema: String?, table: String?, unique: Boolean, approximate: Boolean): ResultSet {
        TODO("not implemented")
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> unwrap(iface: Class<T>): T {
        if (isWrapperFor(iface)) return this as T
        throw SQLException()
    }

    override fun isWrapperFor(iface: Class<*>): Boolean {
        return iface == ru.acapella.db.jdbc.DatabaseMetaData::class.java
    }
}