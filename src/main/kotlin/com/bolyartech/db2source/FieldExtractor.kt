package com.bolyartech.db2source

import java.sql.Connection

class FieldExtractor {
    private val SQL =
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, extra FROM INFORMATION_SCHEMA.columns WHERE table_name = ?"

    fun extract(dbc: Connection, tableName: String): FieldExtractResult {
        val fields = ArrayList<Field>()

        val psLoad = dbc.prepareStatement(SQL)
        psLoad.use {
            psLoad.setString(1, tableName)

            psLoad.executeQuery().use {
                while (it.next()) {
                    try {
                        val len: Long = if (it.getLong(3) != 0L) it.getLong(3) else it.getLong(4)
                        fields.add(Field(it.getString(1), typeMapper(it.getString(2)), len))
                    } catch (e: IllegalArgumentException) {
                        return FieldExtractResultError("Cannot map '{$it.getString(2)}'")
                    }
                }
            }
        }

        return FieldExtractResultOk(fields)
    }

    private fun typeMapper(sqlType: String): FieldType {
        val sqlTypeL = sqlType.toLowerCase()
        return when (sqlTypeL) {
            "int", "mediumint", "smallint", "tinyint" -> FieldType.INT
            "bigint" -> FieldType.LONG
            "boolean" -> FieldType.BOOLEAN
            "varchar" -> FieldType.STRING
            "text", "longtext", "mediumtext", "tinytext" -> FieldType.STRING
            "float" -> FieldType.FLOAT
            "double" -> FieldType.DOUBLE
            "time" -> FieldType.LOCAL_TIME
            "datetime" -> FieldType.LOCAL_DATETIME
            "date" -> FieldType.LOCAL_DATE
            "timestamp" -> FieldType.LONG
            else -> throw IllegalArgumentException()
        }
    }
}

sealed class FieldExtractResult
data class FieldExtractResultOk(val fields: List<Field>) : FieldExtractResult()
class FieldExtractResultError(val reason: String) : FieldExtractResult()