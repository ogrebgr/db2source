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

        val fe = FieldExtractor()
        val fieldsRez = dbc.use {
            fe.extract(dbc, data.tables[0].tableName)
        }

        if (fieldsRez is FieldExtractResultError) {
            return GenerationResultErrorCannotConnectDb(fieldsRez.reason)
        }

        val fields = (fieldsRez as FieldExtractResultOk).fields

        val fac = VelocityTemplateEngineFactory("velocity/kotlin/")

        val tple = fac.createNew()
        tple.assign("class_name", data.tables[0].destinationClassName)
        tple.assign("table_name", data.tables[0].tableName)
        tple.assign("fields", fields)
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

}

sealed class GenerationResult

class GenerationResultOk : GenerationResult()

sealed class GenerationResultError(val reason: String) : GenerationResult()

class GenerationResultErrorCannotConnectDb(reason: String) : GenerationResultError(reason)
class GenerationResultErrorUnableToExtractFields(reason: String) : GenerationResultError(reason)
