package com.bolyartech.db2source

interface TypeMapper {
    fun map(sqlType: String): FieldType
}

class TypeMapperMysql : TypeMapper {
    override fun map(sqlType: String): FieldType {
        val sqlTypeL = sqlType.toLowerCase()
        return when (sqlTypeL) {
            "int", "mediumint", "smallint" -> FieldType.INT
            "bigint" -> FieldType.LONG
            "boolean", "tinyint" -> FieldType.BOOLEAN
            "varchar" -> FieldType.STRING
            "text", "longtext", "mediumtext", "tinytext" -> FieldType.STRING
            "float" -> FieldType.FLOAT
            "double" -> FieldType.DOUBLE
            "time" -> FieldType.LOCAL_TIME
            "datetime" -> FieldType.LOCAL_DATETIME
            "date" -> FieldType.LOCAL_DATE
            "timestamp" -> FieldType.LOCAL_DATETIME
            "binary", "varbinary" -> FieldType.BYTE_ARRAY
            else -> throw IllegalArgumentException()
        }
    }
}

class TypeMapperPostgres : TypeMapper {
    override fun map(sqlType: String): FieldType {
        val sqlTypeL = sqlType.toLowerCase()
        return when (sqlTypeL) {
            "integer", "smallint" -> FieldType.INT
            "bigint" -> FieldType.LONG
            "boolean", "tinyint" -> FieldType.BOOLEAN
            "character varying", "varchar", "character", "char", "text" -> FieldType.STRING
            "timestamp", "timestamp without time zone" -> FieldType.LOCAL_DATETIME
            "timestamp with time zone" -> FieldType.OFFSET_DATATIME
            "date" -> FieldType.LOCAL_DATE
            "time", "time with time zone", "time without time zone" -> FieldType.LOCAL_TIME
            "real" -> FieldType.FLOAT
            "double precision" -> FieldType.DOUBLE
            "bytea" -> FieldType.BYTE_ARRAY
            else -> throw IllegalArgumentException()
        }
    }
}
