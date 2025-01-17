package com.bolyartech.db2source

import VelocityTemplateEngineFactory
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException


class Db2Source {
    private val TEMPLATE_CLASS = "class.vm"

    fun generate(data: ConfigData, copyPreparedStatementExtensions: Boolean = true): GenerationResult {
        val dbc: Connection = try {
            DriverManager.getConnection(data.dbDsn, data.dbUsername, data.dbPassword)
        } catch (e: SQLException) {
            return GenerationResultErrorCannotConnectDb(e.message!!)
        }

        val typeMapper = resolveTypeMapper(data.dbDsn)
        if (typeMapper == null) {
            return GenerationResultErrorUnsupportedDbType(data.dbDsn)
        }

        val fe = FieldExtractor(typeMapper)
        val fieldsRez = dbc.use {
            fe.extract(dbc, data.dbSchema, data.tables[0].tableName)
        }

        if (fieldsRez is FieldExtractResultError) {
            return GenerationResultErrorCannotConnectDb(fieldsRez.reason)
        }

        val fields = (fieldsRez as FieldExtractResultOk).fields

        val fac = VelocityTemplateEngineFactory("velocity/kotlin/")

        val tple = fac.createNew()
        tple.assign("class_name", data.tables[0].destinationClassName)
        tple.assign("db_schema", data.dbSchema)
        tple.assign("table_name", data.tables[0].tableName)
        tple.assign("fields", fields)
        tple.assign("hasIdColumn", fieldsRez.hasId)
        tple.assign("idColumnType", fieldsRez.idType)
        tple.assign("addPaginationMethods", data.addPaginationMethods)
        tple.assign("createValueClassForId", data.createValueClassForId)
        if (!fieldsRez.hasId && data.createValueClassForId) {
            throw IllegalStateException("'createValueClassForId' is ON but there is no 'id' column")
        }
        val src = tple.render(TEMPLATE_CLASS)

        val filename = data.tables[0].destinationClassName + ".kt"
        val file = File(data.tables[0].destinationDir, filename)
        file.printWriter().use {
            it.print(src)
        }

        if (copyPreparedStatementExtensions) {
            val fileContent =
                Db2Source::class.java.getResource("/velocity/kotlin/PreparedStatementExtensions.kt").readText()
            val file2 = File(data.tables[0].destinationDir, "PreparedStatementExtensions.kt")
            file2.printWriter().use {
                it.print(fileContent)
            }
        }

        return GenerationResultOk()
    }

    private fun resolveTypeMapper(dbDsn: String): TypeMapper? {
        return if (dbDsn.startsWith("jdbc:mysql")) {
            TypeMapperMysql()
        } else if (dbDsn.startsWith("jdbc:postgresql")) {
            TypeMapperPostgres()
        } else {
            null
        }
    }

}

sealed class GenerationResult

class GenerationResultOk : GenerationResult()

sealed class GenerationResultError(val reason: String) : GenerationResult()

class GenerationResultErrorUnsupportedDbType(reason: String) : GenerationResultError(reason)
class GenerationResultErrorCannotConnectDb(reason: String) : GenerationResultError(reason)
class GenerationResultErrorUnableToExtractFields(reason: String) : GenerationResultError(reason)
