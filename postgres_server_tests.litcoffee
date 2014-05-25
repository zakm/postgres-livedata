
    Tinytest.add "Postgres - PgCollection config", (test) ->
        connString1 = "postgres://user:pass@host/db"

        PgCollection.config
            connection: connString1

        collection1 = new PgCollection

        test.equal collection1.config.connection, connString1

        connString2 = "postgres://user:pass@host/db2"
        collection2 = new PgCollection "test",
            connection: connString2

        test.equal collection2.config.connection, connString2

    Tinytest.add "Postgres - Escapes", (test) ->

        escapeName = PgCollection.escapeName
        escapeValue = PgCollection.escapeValue

        test.equal escapeName('foo"bar'), '"foo""bar"'
        test.equal escapeValue("foo'bar"), "'foo''bar'"

    Tinytest.add "Postgres - Queries", (test) ->

        select_all = PgCollection.selectorToQuery "name", {}
        test.equal select_all, 'SELECT * FROM "name"'

        select_where_string = PgCollection.selectorToQuery "name", { foo: "bar" }
        test.equal select_where_string, 'SELECT * FROM "name" WHERE "foo" = \'bar\''

        select_where_number = PgCollection.selectorToQuery "name", { foo: 2 }
        test.equal select_where_number, 'SELECT * FROM "name" WHERE "foo" = 2'
