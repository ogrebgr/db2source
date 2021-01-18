package com.bolyartech.db2source

data class Field(val name: String, val codeName: String, val type: FieldType, val len: Long)


enum class FieldType {
    INT,
    LONG,
    STRING,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    LOCAL_TIME,
    LOCAL_DATE,
    LOCAL_DATETIME,
    OFFSET_DATATIME,
    BYTE_ARRAY
}