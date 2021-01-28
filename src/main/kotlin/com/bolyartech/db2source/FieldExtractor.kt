package com.bolyartech.db2source

import org.apache.commons.lang.WordUtils
import java.sql.Connection
import java.sql.SQLException

class FieldExtractor(private val typeMapper: TypeMapper) {
    private val SQL =
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, is_nullable FROM INFORMATION_SCHEMA.columns " +
                "WHERE table_schema = ? AND table_name = ?"

    fun extract(dbc: Connection, schema: String, tableName: String): FieldExtractResult {
        val fields = ArrayList<Field>()

        val psLoad = dbc.prepareStatement(SQL)
        psLoad.use {
            psLoad.setString(1, schema)
            psLoad.setString(2, tableName)

            try {
                psLoad.executeQuery().use {
                    while (it.next()) {
                        try {

                            val len: Long = if (it.getLong(3) != 0L) it.getLong(3) else it.getLong(4)
                            fields.add(
                                Field(
                                    it.getString(1),
                                    WordUtils.uncapitalize(
                                        WordUtils.capitalize(it.getString(1), charArrayOf('_')).replace("_", "")
                                    ),
                                    typeMapper.map(it.getString(2)),
                                    len,
                                    it.getString(5) == "YES"
                                )
                            )
                        } catch (e: IllegalArgumentException) {
                            return FieldExtractResultError("Cannot map '${it.getString(2)}'")
                        }
                    }
                }
            } catch (e: SQLException) {
                FieldExtractResultError(e.message ?: "")
            }
        }

        return FieldExtractResultOk(fields)
    }
}

sealed class FieldExtractResult
data class FieldExtractResultOk(val fields: List<Field>) : FieldExtractResult()
class FieldExtractResultError(val reason: String) : FieldExtractResult()