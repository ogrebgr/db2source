package com.bolyartech.db2source

import setValue
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime


data class Test(
    val id: Int,
    val intcol: Int,
    val varcharcol: String,
    val textcol: String,
    val timecol: LocalTime,
    val datetimecol: LocalDateTime,
    val floatcol: Float,
    val doublecol: Double,
    val datecol: LocalDate,
    val tscol: OffsetDateTime,
    val longtextcol: String,
    val booleancol: Boolean
)

// TODO add provider to Dagger DI
interface TestDbh {
    @Throws(SQLException::class)
    fun createNew(
        dbc: Connection,
        intcol: Int,
        varcharcol: String,
        textcol: String,
        timecol: LocalTime,
        datetimecol: LocalDateTime,
        floatcol: Float,
        doublecol: Double,
        datecol: LocalDate,
        tscol: OffsetDateTime,
        longtextcol: String,
        booleancol: Boolean
    ): Test

    @Throws(SQLException::class)
    fun loadById(dbc: Connection, id: Int): Test?

    @Throws(SQLException::class)
    fun loadAll(dbc: Connection): List<Test>

    @Throws(SQLException::class)
    fun update(dbc: Connection, obj: Test): Boolean

    @Throws(SQLException::class)
    fun loadPageIdGreater(dbc: Connection, idGreater: Int, pageSize: Int): List<Test>

    @Throws(SQLException::class)
    fun loadPageIdLower(dbc: Connection, idLower: Int, pageSize: Int): List<Test>

    @Throws(SQLException::class)
    fun loadLastPage(dbc: Connection, pageSize: Int): List<Test>

    @Throws(SQLException::class)
    fun count(dbc: Connection): Int

    @Throws(SQLException::class)
    fun delete(dbc: Connection, id: Int): Int

    @Throws(SQLException::class)
    fun deleteAll(dbc: Connection): Int
}

class TestDbhImpl : TestDbh {
    private val SQL_INSERT =
        """INSERT INTO "test"."test" ("intcol", "varcharcol", "textcol", "timecol", "datetimecol", "floatcol", "doublecol", "datecol", "tscol", "longtextcol", "booleancol") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    private val SQL_SELECT_BY_ID =
        """SELECT "intcol", "varcharcol", "textcol", "timecol", "datetimecol", "floatcol", "doublecol", "datecol", "tscol", "longtextcol", "booleancol" FROM "test"."test" WHERE id = ?"""
    private val SQL_SELECT_ALL =
        """SELECT "id", "intcol", "varcharcol", "textcol", "timecol", "datetimecol", "floatcol", "doublecol", "datecol", "tscol", "longtextcol", "booleancol" FROM "test"."test""""
    private val SQL_UPDATE =
        """UPDATE "test"."test" SET "intcol" = ?, "varcharcol" = ?, "textcol" = ?, "timecol" = ?, "datetimecol" = ?, "floatcol" = ?, "doublecol" = ?, "datecol" = ?, "tscol" = ?, "longtextcol" = ?, "booleancol" = ? WHERE id = ?"""
    private val SQL_SELECT_ID_GREATER =
        """SELECT "id", "intcol", "varcharcol", "textcol", "timecol", "datetimecol", "floatcol", "doublecol", "datecol", "tscol", "longtextcol", "booleancol" FROM "test"."test" WHERE id > ?"""
    private val SQL_SELECT_ID_LOWER =
        """SELECT "id", "intcol", "varcharcol", "textcol", "timecol", "datetimecol", "floatcol", "doublecol", "datecol", "tscol", "longtextcol", "booleancol" FROM "test"."test" WHERE id < ?"""
    private val SQL_SELECT_LAST =
        """SELECT "id", "intcol", "varcharcol", "textcol", "timecol", "datetimecol", "floatcol", "doublecol", "datecol", "tscol", "longtextcol", "booleancol" FROM "test"."test" ORDER BY id DESC"""
    private val SQL_COUNT = """SELECT COUNT(id) FROM "test"."test""""
    private val SQL_DELETE = """DELETE FROM "test"."test" WHERE id = ?"""
    private val SQL_DELETE_ALL = """DELETE FROM "test"."test""""

    @Throws(SQLException::class)
    override fun createNew(
        dbc: Connection,
        intcol: Int,
        varcharcol: String,
        textcol: String,
        timecol: LocalTime,
        datetimecol: LocalDateTime,
        floatcol: Float,
        doublecol: Double,
        datecol: LocalDate,
        tscol: OffsetDateTime,
        longtextcol: String,
        booleancol: Boolean
    ): Test {
        dbc.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS).use {
            it.setValue(1, intcol)
            it.setValue(2, varcharcol)
            it.setValue(3, textcol)
            it.setValue(4, timecol)
            it.setValue(5, datetimecol)
            it.setValue(6, floatcol)
            it.setValue(7, doublecol)
            it.setValue(8, datecol)
            it.setValue(9, tscol)
            it.setValue(10, longtextcol)
            it.setValue(11, booleancol)

            it.executeUpdate()
            it.generatedKeys.use {
                it.next()
                return Test(
                    it.getInt(1),
                    intcol,
                    varcharcol,
                    textcol,
                    timecol,
                    datetimecol,
                    floatcol,
                    doublecol,
                    datecol,
                    tscol,
                    longtextcol,
                    booleancol
                )
            }
        }
    }


    @Throws(SQLException::class)
    override fun loadById(dbc: Connection, id: Int): Test? {
        dbc.prepareStatement(SQL_SELECT_BY_ID).use {
            it.setValue(1, id)

            it.executeQuery().use {
                return if (it.next()) {
                    Test(
                        id,
                        it.getInt(1),
                        it.getString(2),
                        it.getString(3),
                        it.getObject(4, LocalTime::class.java),
                        it.getObject(5, LocalDateTime::class.java),
                        it.getFloat(6),
                        it.getDouble(7),
                        it.getObject(8, LocalDate::class.java),
                        it.getObject(9, OffsetDateTime::class.java),
                        it.getString(10),
                        it.getBoolean(11)
                    )
                } else {
                    null
                }
            }
        }
    }

    @Throws(SQLException::class)
    override fun loadAll(dbc: Connection): List<Test> {
        val ret = ArrayList<Test>()
        dbc.prepareStatement(SQL_SELECT_ALL).use {
            it.executeQuery().use {
                while (it.next()) {
                    ret.add(
                        Test(
                            it.getInt(1),
                            it.getInt(2),
                            it.getString(3),
                            it.getString(4),
                            it.getObject(5, LocalTime::class.java),
                            it.getObject(6, LocalDateTime::class.java),
                            it.getFloat(7),
                            it.getDouble(8),
                            it.getObject(9, LocalDate::class.java),
                            it.getObject(10, OffsetDateTime::class.java),
                            it.getString(11),
                            it.getBoolean(12)
                        )
                    )
                }
            }
        }

        return ret
    }

    @Throws(SQLException::class)
    override fun update(dbc: Connection, obj: Test): Boolean {
        dbc.prepareStatement(SQL_UPDATE).use {
            it.setValue(1, obj.intcol)
            it.setValue(2, obj.varcharcol)
            it.setValue(3, obj.textcol)
            it.setValue(4, obj.timecol)
            it.setValue(5, obj.datetimecol)
            it.setValue(6, obj.floatcol)
            it.setValue(7, obj.doublecol)
            it.setValue(8, obj.datecol)
            it.setValue(9, obj.tscol)
            it.setValue(10, obj.longtextcol)
            it.setValue(11, obj.booleancol)
            it.setValue(12, obj.id)

            return it.executeUpdate() > 0
        }
    }

    @Throws(SQLException::class)
    override fun loadPageIdGreater(dbc: Connection, idGreater: Int, pageSize: Int): List<Test> {
        if (pageSize <= 0) {
            throw IllegalArgumentException("pageSize <= 0")
        }

        dbc.prepareStatement(SQL_SELECT_ID_GREATER).use {
            it.setValue(1, idGreater)

            it.executeQuery().use {
                var count = 0
                val ret = ArrayList<Test>()

                while (it.next()) {
                    ret.add(
                        Test(
                            it.getInt(1),
                            it.getInt(2),
                            it.getString(3),
                            it.getString(4),
                            it.getObject(5, LocalTime::class.java),
                            it.getObject(6, LocalDateTime::class.java),
                            it.getFloat(7),
                            it.getDouble(8),
                            it.getObject(9, LocalDate::class.java),
                            it.getObject(10, OffsetDateTime::class.java),
                            it.getString(11),
                            it.getBoolean(12)
                        )
                    )
                    count++
                    if (count == pageSize) {
                        return ret
                    }
                }

                return ret
            }
        }
    }


    @Throws(SQLException::class)
    override fun loadPageIdLower(dbc: Connection, idLower: Int, pageSize: Int): List<Test> {
        if (pageSize <= 0) {
            throw IllegalArgumentException("pageSize <= 0")
        }

        dbc.prepareStatement(SQL_SELECT_ID_LOWER).use {
            it.setValue(1, idLower)

            it.executeQuery().use {
                var count = 0
                val ret = ArrayList<Test>()

                while (it.next()) {
                    ret.add(
                        Test(
                            it.getInt(1),
                            it.getInt(2),
                            it.getString(3),
                            it.getString(4),
                            it.getObject(5, LocalTime::class.java),
                            it.getObject(6, LocalDateTime::class.java),
                            it.getFloat(7),
                            it.getDouble(8),
                            it.getObject(9, LocalDate::class.java),
                            it.getObject(10, OffsetDateTime::class.java),
                            it.getString(11),
                            it.getBoolean(12)
                        )
                    )
                    count++
                    if (count == pageSize) {
                        return ret
                    }
                }

                return ret
            }
        }
    }

    @Throws(SQLException::class)
    override fun loadLastPage(dbc: Connection, pageSize: Int): List<Test> {
        val ret = ArrayList<Test>()
        dbc.prepareStatement(SQL_SELECT_LAST).use {
            it.executeQuery().use {
                var count = 0
                while (it.next()) {
                    ret.add(
                        Test(
                            it.getInt(1),
                            it.getInt(2),
                            it.getString(3),
                            it.getString(4),
                            it.getObject(5, LocalTime::class.java),
                            it.getObject(6, LocalDateTime::class.java),
                            it.getFloat(7),
                            it.getDouble(8),
                            it.getObject(9, LocalDate::class.java),
                            it.getObject(10, OffsetDateTime::class.java),
                            it.getString(11),
                            it.getBoolean(12)
                        )
                    )
                    count++
                    if (count == pageSize) {
                        return ret
                    }
                }
            }
        }

        return ret
    }

    @Throws(SQLException::class)
    override fun count(dbc: Connection): Int {
        dbc.prepareStatement(SQL_COUNT).use {
            it.executeQuery().use {
                it.next()
                return it.getInt(1)
            }
        }
    }

    @Throws(SQLException::class)
    override fun delete(dbc: Connection, id: Int): Int {
        dbc.prepareStatement(SQL_DELETE).use {
            it.setValue(1, id)
            return it.executeUpdate()
        }
    }

    @Throws(SQLException::class)
    override fun deleteAll(dbc: Connection): Int {
        dbc.prepareStatement(SQL_DELETE_ALL).use {
            return it.executeUpdate()
        }
    }
}