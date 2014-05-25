
    coll = null

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


    Tinytest.add "Postgres - Setup", (test) ->
        coll = new PgCollection "user",
            connection: "postgres://zakm@localhost/test"

        coll.exec \
            'CREATE TABLE IF NOT EXISTS "user" (
                id    UUID PRIMARY KEY,
                email TEXT NOT NULL,
                pass  TEXT NOT NULL
            )'


    Tinytest.add "Postgres - Insert", (test) ->

        coll.insert [
            { id: Meteor.uuid(), email: "alice@example.com", pass: "password" }
            { id: Meteor.uuid(), email: "bob@example.com", pass: "abc123" }
            { id: Meteor.uuid(), email: "carol@example.com", pass: "123456" }
            { id: Meteor.uuid(), email: "dan@example.com", pass: "123123" }
            { id: Meteor.uuid(), email: "eve@example.com", pass: "qwerty" }
        ]

    Tinytest.add "Postgres - Update", (test) ->

        coll.update { email: "eve@example.com" }, { pass: "ekrpat" }

    Tinytest.add "Postgres - Select", (test) ->

        {rows} = coll.find { email: "alice@example.com" }
        test.equal rows[0].pass, "password"

        {rows} = coll.find { email: "eve@example.com" }
        test.equal rows[0].pass, "ekrpat"

    Tinytest.add "Postgres - Teardown", (test) ->

        coll.exec 'DROP TABLE "user"'
