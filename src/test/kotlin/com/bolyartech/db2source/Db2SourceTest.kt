package com.bolyartech.db2source

import org.junit.Assert.assertTrue
import org.junit.Test
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class Db2SourceTest {
    private val poko = Test(
        1, 1, "aaa", "bbb",
        LocalTime.of(11, 12), LocalDateTime.of(2020, 1, 21, 21, 22), 1.2f, 1.3,
        LocalDate.of(2020, 2, 22), LocalDateTime.of(2020, 1, 21, 21, 22), "ccc", true
    )

    @Test
    fun test_Generate() {
        Class.forName("com.mysql.jdbc.Driver")
        val conf = ConfigData(
            "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
            "test", "test", "test", listOf(TableConfig("test", "Test", "/tmp"))
        )
        val db2source = Db2Source()
        db2source.generate(conf)

        // No exception is thrown
        assertTrue(true)
    }

    @Test
    fun test_createNew() {
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()
        val newObj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
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
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()

        val newObj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
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
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)
        var count = impl.count(dbc)
        assertTrue(count == 0)

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )
        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        count = impl.count(dbc)
        assertTrue(count == 2)
    }


    @Test
    fun test_update() {
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()

        val obj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )
        val newintcol = 456
        val newObj = Test(
            obj.id, newintcol, obj.varcharcol, obj.textcol,
            obj.timecol, obj.datetimecol, obj.floatcol, obj.doublecol,
            obj.datecol, obj.tscol, obj.longtextcol, obj.booleancol
        )
        val rez = impl.update(dbc, newObj)
        assertTrue(rez)

        val loaded = impl.loadById(dbc, obj.id)
        assertTrue(loaded!!.intcol == 456)
    }


    @Test
    fun test_loadPageIdGreater() {
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        val obj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        val gt = impl.loadPageIdGreater(dbc, obj.id, 20)
        assertTrue(gt.size == 2)
    }

    @Test
    fun test_loadPageIdLower() {
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        val obj = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        val gt = impl.loadPageIdLower(dbc, obj.id, 20)
        assertTrue(gt.size == 2)
    }

    @Test
    fun test_loadLastPage() {
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        val gt = impl.loadLastPage(dbc, 2)
        assertTrue(gt.size == 2)
    }

    @Test
    fun test_count() {
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        assert(impl.count(dbc) == 0)
        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )
        assert(impl.count(dbc) == 1)
        impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )
        assert(impl.count(dbc) == 2)
    }

    @Test
    fun test_delete() {
        Class.forName("com.mysql.jdbc.Driver")

        val dbc: Connection =
            DriverManager.getConnection(
                "jdbc:mysql://localhost/test?verifyServerCertificate=false&useSSL=false&useUnicode=true&characterEncoding=utf-8",
                "test", "test"
            )

        val impl = TestDbhImpl()
        impl.deleteAll(dbc)

        val obj1 = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )
        val obj2 = impl.createNew(
            dbc, poko.intcol, poko.varcharcol, poko.textcol,
            poko.timecol, poko.datetimecol, poko.floatcol, poko.doublecol,
            poko.datecol, poko.tscol, poko.longtextcol, poko.booleancol
        )

        assertTrue(impl.delete(dbc, obj1.id) == 1)

        val left = impl.loadAll(dbc)
        assertTrue(left.size == 1)

        assertTrue(left[0].id == obj2.id)
    }
}


