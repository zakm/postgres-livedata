
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
        coll = new PgCollection ["test","user"],
            connection: "postgres://zakm@localhost/test"

        coll.exec \
            'CREATE SCHEMA test;
             CREATE TABLE test.user (
                 id     UUID PRIMARY KEY,
                 email  TEXT    NOT NULL,
                 pass   TEXT    NOT NULL,
                 logins INTEGER DEFAULT 1
             )'


    Tinytest.add "Postgres - Insert", (test) ->

        coll.insert [
            { id: Meteor.uuid(), email: "alice@example.com", pass: "password" }
            { id: Meteor.uuid(), email: "bob@example.com",   pass: "abc123" }
            { id: Meteor.uuid(), email: "carol@example.com", pass: "123456" }
            { id: Meteor.uuid(), email: "dan@example.com",   pass: "123123" }
            { id: Meteor.uuid(), email: "eve@example.com",   pass: "qwerty" }
            { id: Meteor.uuid(), email: "frank@example.com", pass: "welcome" }
            { id: Meteor.uuid(), email: "gertrude@example.com", pass: "123456789" }
        ]

    Tinytest.add "Postgres - Update", (test) ->

        coll.update { email: "alice@example.com" }, { $inc: { logins: 1 } }
        coll.update { email: "bob@example.com" },   { $mul: { logins: 3 } }
        coll.update { email: "carol@example.com" }, { $set: { pass: "secure" } }
        coll.update { email: "dan@example.com" },   { $unset: { logins: 1 } }
        coll.update { email: "eve@example.com" },   { pass: "ekrpat" }
        coll.update { email: "frank@example.com" }, { $min: { logins: 0 } }
        coll.update { email: "gertrude@example.com" }, { $max: { logins: 5 } }

    Tinytest.add "Postgres - Select", (test) ->

        curs = coll.find { email: "alice@example.com" }
        test.equal curs.next().pass, "password"

        curs = coll.find { email: "bob@example.com" }
        test.equal curs.next().logins, 3

        curs = coll.find { email: "carol@example.com" }
        test.equal curs.next().pass, "secure"

        curs = coll.find { email: "dan@example.com" }
        test.isNull curs.next().logins

        curs = coll.find { email: "eve@example.com" }
        test.equal curs.next().pass, "ekrpat"

        curs = coll.find { email: "frank@example.com" }
        test.equal curs.next().logins, 0

        curs = coll.find { email: "gertrude@example.com" }
        test.equal curs.next().logins, 5

    Tinytest.add "Postgres - Notify", (test) ->



    Tinytest.add "Postgres - Teardown", (test) ->

        coll.exec 'DROP SCHEMA test CASCADE'
