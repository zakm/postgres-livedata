
    user = null
    email = null

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
        user = new PgCollection ["test","user"],
            connection: "postgres://localhost/test"
            references:
                email:
                    table: ["test","email"]
                    its: "userid"
                    my: "id"

        # doing a user.find() should generate the following query:
        #
        # SELECT "user".*, json_agg("email") AS "email" FROM "user"
        # LEFT JOIN "email" ON ("email"."userid" = "user"."id") GROUP BY "user"."id";

        # doing a user.insert({ name: "Bob", email: [{ address: "bob@example.com" }] })
        # should produce:
        #
        # WITH "a" AS (INSERT INTO "user" ("name") VALUES ('Bob') RETURNING *)
        # INSERT INTO "email" ("userid","address") SELECT "a"."id", "b".* FROM "a"
        # CROSS JOIN (VALUES ('bob@example.com')) "b";

        user.transact ->
            @exec 'CREATE SCHEMA test'

            @exec \
                'CREATE TABLE test.user (
                    id     SERIAL  PRIMARY KEY,
                    name   TEXT    NOT NULL,
                    pass   TEXT    NOT NULL,
                    logins INTEGER DEFAULT 1
                )'

            @exec \
                'CREATE TABLE test.email (
                    address   TEXT    PRIMARY KEY,
                    userid    INTEGER NOT NULL    REFERENCES test.user (id),
                    "primary" BOOLEAN NOT NULL DEFAULT false
                )'


    Tinytest.add "Postgres - Insert", (test) ->

        user.insert [
            { name: "Alice Cooper",    pass: "password" }
            { name: "Bob Ross",        pass: "abc123",    email: [{ address: "bob@example.com", primary: true }] }
            { name: "Carol Lombard",   pass: "123456",    email: [{ address: "carol@example.com" }] }
            { name: "Dan Ackroyd",     pass: "123123",    email: [{ address: "dan@example.com", primary: true }] }
            { name: "Eve",             pass: "qwerty",    email: [{ address: "eve@example.com" }, { address: "eva@example.com", primary: true }] }
            { name: "Freddie Mercury", pass: "welcome",   email: [{ address: "fred@example.com", primary: true }] }
            { name: "Gertrude Stein",  pass: "123456789" }
        ]



    Tinytest.add "Postgres - Update", (test) ->

        user.update { id: 1 }, { $inc: { logins: 1 } }
        user.update { id: 2 }, { $mul: { logins: 3 } }
        user.update { id: 3 }, { $set: { pass: "secure" } }
        user.update { id: 4 }, { $unset: { logins: 1 } }
        user.update { id: 5 }, { pass: "ekrpat" }
        user.update { id: 6 }, { $min: { logins: 0 } }
        user.update { id: 7 }, { $max: { logins: 5 } }

    Tinytest.add "Postgres - Select", (test) ->

        curs = user.find { id: 1 }
        test.equal curs.next().pass, "password"

        curs = user.find { id: 2 }
        test.equal curs.next().logins, 3

        curs = user.find { id: 3 }
        test.equal curs.next().pass, "secure"

        curs = user.find { id: 4 }
        test.isNull curs.next().logins

        curs = user.find { id: 5 }
        test.equal curs.next().pass, "ekrpat"

        curs = user.find { id: 6 }
        test.equal curs.next().logins, 0

        curs = user.find { id: 7 }
        test.equal curs.next().logins, 5

    Tinytest.add "Postgres - Notify", (test) ->



    Tinytest.add "Postgres - Teardown", (test) ->

        user.exec 'DROP SCHEMA test CASCADE'
