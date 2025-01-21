package com.bolyartech.db2source

data class ConfigData(
    val dbDsn: String,
    val dbUsername: String,
    val dbPassword: String,
    val dbSchema: String,
    val tables: List<TableConfig>,
    val addPaginationMethods: Boolean,
    val createValueClassForId: Boolean,
    val addDependencyInjectionCode: Boolean,
)

data class TableConfig(val tableName: String, val destinationClassName: String, val destinationDir: String)
