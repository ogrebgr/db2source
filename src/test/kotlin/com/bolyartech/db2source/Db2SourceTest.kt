package com.bolyartech.db2source

import org.junit.Assert.assertTrue
import org.junit.Test
import java.sql.Connection
import java.sql.DriverManager
import java.time.*

class Db2SourceTest {
    //    private val dsn = "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8"
    private val dsn = "jdbc:postgresql://localhost:5432/postgres"

    init {
        Class.forName("com.mysql.jdbc.Driver")
        Class.forName("org.postgresql.Driver")
    }

    private val poko = com.bolyartech.db2source.Test(
        1, 1, "aaa", "bbb",
        LocalTime.of(11, 12), LocalDateTime.of(2020, 1, 21, 21, 22), 1.2f, 1.3,
        LocalDate.of(2020, 2, 22), OffsetDateTime.of(2020, 1, 21, 21, 22, 0, 0, ZoneOffset.UTC), "ccc", true,
        byteArrayOf(0x01, 0x02, 0x03)
    )

    @Test
    fun test_Generate() {
        val conf = ConfigData(
            dsn,
            "test", "test", "test", listOf(TableConfig("test", "Test", "/tmp"))
        )
        val db2source = Db2Source()
        db2source.generate(conf)

        // No exception is thrown so we are good
        assertTrue(true)
    }

    @Test
    fun test_createNew() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()
        val newObj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        assertTrue(newObj.intcol == poko.intcol)
        assertTrue(newObj.varcharcol == poko.varcharcol)
        assertTrue(newObj.textcol == poko.textcol)
        assertTrue(newObj.timecol == poko.timecol)
        assertTrue(newObj.datetimecol == poko.datetimecol)
        assertTrue(newObj.datecol == poko.datecol)
        assertTrue(newObj.floatcol == poko.floatcol)
        assertTrue(newObj.doublecol == poko.doublecol)
        assertTrue(newObj.tscol == poko.tscol)
        assertTrue(newObj.longtextcol == poko.longtextcol)
        assertTrue(newObj.booleancol == poko.booleancol)
    }

    @Test
    fun test_loadById() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()

        val newObj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        val loaded = impl.loadById(dbc, newObj.id)
        assertTrue(loaded!!.intcol == poko.intcol)
        assertTrue(loaded.varcharcol == poko.varcharcol)
        assertTrue(loaded.textcol == poko.textcol)
        assertTrue(loaded.timecol == poko.timecol)
        assertTrue(loaded.datetimecol == poko.datetimecol)
        assertTrue(loaded.datecol == poko.datecol)
        assertTrue(loaded.floatcol == poko.floatcol)
        assertTrue(loaded.doublecol == poko.doublecol)
        assertTrue(loaded.tscol == poko.tscol)
        assertTrue(loaded.longtextcol == poko.longtextcol)
        assertTrue(loaded.booleancol == poko.booleancol)
    }

    @Test
    fun test_loadAll() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)
        var count = impl.count(dbc)
        assertTrue(count == 0)

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )
        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        count = impl.count(dbc)
        assertTrue(count == 2)
    }


    @Test
    fun test_update() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()

        val obj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )
        val newintcol = 456
        val newObj = Test(
            obj.id, newintcol, obj.varcharcol, obj.textcol,
            obj.timecol, obj.datetimecol, obj.floatcol, obj.doublecol,
            obj.datecol, obj.tscol, obj.longtextcol, obj.booleancol, obj.bytecol
        )
        val rez = impl.update(dbc, newObj)
        assertTrue(rez)

        val loaded = impl.loadById(dbc, obj.id)
        assertTrue(loaded!!.intcol == 456)
    }


    @Test
    fun test_loadPageIdGreater() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        val obj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        val gt = impl.loadPageIdGreater(dbc, obj.id, 20)
        assertTrue(gt.size == 2)
    }

    @Test
    fun test_loadPageIdLower() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        val obj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        val gt = impl.loadPageIdLower(dbc, obj.id, 20)
        assertTrue(gt.size == 2)
    }

    @Test
    fun test_loadLastPage() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        val gt = impl.loadLastPage(dbc, 2)
        assertTrue(gt.size == 2)
    }

    @Test
    fun test_count() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        assert(impl.count(dbc) == 0)
        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )
        assert(impl.count(dbc) == 1)
        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )
        assert(impl.count(dbc) == 2)
    }

    @Test
    fun test_delete() {
        val dbc: Connection =
            DriverManager.getConnection(
                dsn,
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        val obj1 = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )
        val obj2 = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol, poko.bytecol
        )

        assertTrue(impl.delete(dbc, obj1.id) == 1)

        val left = impl.loadAll(dbc)
        assertTrue(left.size == 1)

        assertTrue(left[0].id == obj2.id)
    }
}


