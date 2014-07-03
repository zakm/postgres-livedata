
    user = null
    mong = null

    Tinytest.add "Postgres - PgCollection config", (test) ->
        connString1 = "postgres://user:pass@host/db"

        # There should be a global config that all instances default to
        PgCollection.config
            connection: connString1

        collection1 = new PgCollection

        test.equal collection1.config.connection, connString1

        # Each instance should be able to specify its own config
        connString2 = "postgres://user:pass@host/db2"
        collection2 = new PgCollection "test",
            connection: connString2

        test.equal collection2.config.connection, connString2

    Tinytest.add "Postgres - Query transforms", (test) ->

        testQuery = (desc, {query,params}, expectedQuery, expectedParams) ->
            test.equal query, expectedQuery, desc
            if expectedParams?
                test.equal params.length, expectedParams.length, "params: " + desc
                _.each expectedParams, (v,i) ->
                    test.equal params[i], v, "params.#{i}: " + desc


# Selects

        fake = _.extend { name: "test" }, PgCollection.prototype

        expect = 'SELECT * FROM "test"'

## Sanity test

        test.equal "select empty object",
            fake.makeSelect( {} ).query,
            expect,
            []

        test.equal "select no arguments",
            fake.makeSelect().query,
            expect,
            []


## Where clause

        expect = "#{expect} WHERE"

        testQuery "select single param",
            fake.makeSelect( id: "abc" ),
            expect + ' "id" = $1',
            ["abc"]

        testQuery "select multiple params",
            fake.makeSelect( id: "abc", foo: "def" ),
            expect + ' "id" = $1 AND "foo" = $2',
            ["abc","def"]

        testQuery "select same params",
            fake.makeSelect( id: "abc", foo: "abc" ),
            expect + ' "id" = $1 AND "foo" = $1',
            ["abc"]


## Comparisons

        testQuery "select $gt",
            fake.makeSelect( noses: { $gt: 1 } ),
            expect + ' "noses" > $1'

        testQuery "select $gte",
            fake.makeSelect( eyes: { $gte: 4 } ),
            expect + ' "eyes" >= $1'

        testQuery "select $lt",
            fake.makeSelect( brains: { $lt: 1 } ),
            expect + ' "brains" < $1'

        testQuery "select $lte",
            fake.makeSelect( toeses: { $lte: 10 } ),
            expect + ' "toeses" <= $1'

        testQuery "select $ne",
            fake.makeSelect( vno: { $ne: 5 } ),
            expect + ' "vno" != $1'

        testQuery "select $in",
            fake.makeSelect( foo: { $in: ['a','b','c'] } ),
            expect + ' "foo" IN ($1,$2,$3)',
            ['a','b','c']

        testQuery "select $nin",
            fake.makeSelect( foo: { $nin: ['d','e','f'] } ),
            expect + ' "foo" NOT IN ($1,$2,$3)',
            ['d','e','f']

## Compounds


        testQuery "select $and",
            fake.makeSelect( $and: [ { foo: "abc" }, { bar: "def" } ] ),
            expect + ' ("foo" = $1) AND ("bar" = $2)',
            ["abc","def"]

        testQuery "select $and nested",
            fake.makeSelect( $and: [ { foo: "abc" }, { bar: "def", baz: "ghi" } ] ),
            expect + ' ("foo" = $1) AND ("bar" = $2 AND "baz" = $3)',
            ["abc","def", "ghi"]

        testQuery "select $or",
            fake.makeSelect( $or: [ { foo: "abc" }, { bar: "def" } ] ),
            expect + ' ("foo" = $1) OR ("bar" = $2)',
            ["abc","def"]

        testQuery "select $or nested",
            fake.makeSelect( $or: [ { foo: "abc" }, { bar: "def", baz: "ghi" } ] ),
            expect + ' ("foo" = $1) OR ("bar" = $2 AND "baz" = $3)',
            ["abc","def","ghi"]


## Negation

        testQuery "select $not multiple",
            fake.makeSelect( $not: [ { foo: "abc" }, { bar: "def" } ] ),
            expect + ' NOT (("foo" = $1) AND ("bar" = $2))',
            ["abc","def"]

        testQuery "select $not nested",
            fake.makeSelect( $not: [ { foo: "abc" }, { bar: "def", baz: "ghi" } ] ),
            expect + ' NOT (("foo" = $1) AND ("bar" = $2 AND "baz" = $3))',
            ["abc","def","ghi"]

        testQuery "select $nor multiple",
            fake.makeSelect( $nor: [ { foo: "abc" }, { bar: "def" } ] ),
            expect + ' NOT (("foo" = $1) OR ("bar" = $2))',
            ["abc","def"]

        testQuery "select $nor nested",
            fake.makeSelect( $nor: [ { foo: "abc" }, { bar: "def", baz: "ghi" } ] ),
            expect + ' NOT (("foo" = $1) OR ("bar" = $2 AND "baz" = $3))',
            ["abc","def","ghi"]

## Exists / IS NULL / IS NOT NULL

        testQuery "select $exists: true",
            fake.makeSelect( bonobo: { $exists: true } )
            expect + ' "bonobo" IS NOT NULL'

        testQuery "select $exists: false",
            fake.makeSelect( rhesus: { $exists: false } ) # there's no right way to eat a rhesus
            expect + ' "rhesus" IS NULL'

## Evaluation

        testQuery "select $mod",
            fake.makeSelect( income: { $mod: [4, 0] } )
            expect + ' "income" % $1 = $2',
            [4, 0]

## Search

### Regex

        testQuery "select $regex case sensitive",
            fake.makeSelect( company: { $regex: /Acme.*Corp/ } )
            expect + ' "company" ~ $1',
            ['Acme.*Corp']

        testQuery "select $regex case INsensitive",
            fake.makeSelect( company: { $regex: /acme.*corp/i } )
            expect + ' "company" ~* $1',
            ['acme.*corp']

        testQuery "select $regex case INsensitive string",
            fake.makeSelect( company: { $regex: 'acme.*corp', $options: 'i' } )
            expect + ' "company" ~* $1',
            ['acme.*corp']

### Text

TODO, maybe

## Array

        testQuery "select $all json array",
            _.extend( { types: { role: 'json' } }, fake ).makeSelect \
                { role: { $all: ['director','producer'] } }
            expect + ' ARRAY(SELECT * FROM JSON_ARRAY_ELEMENTS("role"))::text[] @> ARRAY[$1,$2]',
            ['director','producer']

        testQuery "select $all array", # TODO, config distinction between array and json array columns
            fake.makeSelect( role: { $all: ['director','producer'] } )
            expect + ' "role" @> ARRAY[$1,$2]',
            ['director','producer']

        testQuery "select $elemMatch", # TODO, think about better handling types
            fake.makeSelect( email: { $elemMatch: { address: "foo@bar.com", primary: true } } )
            expect + ' ("email"->>$1 = $2 AND "email"->>$3 = $4)',
            ['address','foo@bar.com','primary',true]

        testQuery "select $size json array",
            _.extend( { types: { elephant: 'json' } }, fake ).makeSelect \
                elephant: { $size: 10 }
            expect + ' JSON_ARRAY_LENGTH("elephant") = $1',
            [10]

        testQuery "select $size array",
            fake.makeSelect( elephant: { $size: 10 } )
            expect + ' ARRAY_LENGTH("elephant") = $1',
            [10]


## Specific fields / columns

        testQuery "select columns",
            fake.makeSelect( {}, fields: { foo: 1, bar: 0, baz: 1 } ),
            'SELECT "test"."foo","test"."baz" FROM "test"'


## Specifing a schema

        testQuery "select schema",
            _.extend( fake, name: ["schema","table"] ).makeSelect({}),
            'SELECT * FROM "schema"."table"'


# Relational selects

`relations: { some_column: { foreign_table: "foreign", foreign_id: "remote", local_id: "id" }` is like declaring
`"some_column" TYPE REFERENCES "foreign" ("remote")` in the SQL table where the primary key is `"id"`

an additional `array_of` treats the remote table like an array of just the specified column

        faker  = _.extend {
            name: "test"
            primary_keys: ["lid"]
            relations:
                some_column:
                    foreign_table: "ftable"
                    foreign_id: "fid"
                    local_id: "lid"
            }, PgCollection.prototype

        fakerc = _.extend {
            name: "test"
            primary_keys: ["lid"]
            relations:
                some_column:
                    foreign_table: "ftable"
                    foreign_id: "fid"
                    local_id: "lid"
                    array_of: "column"
            }, PgCollection.prototype

        expect = 'SELECT "test".*,JSON_AGG(DISTINCT "ftable".*) "some_column" FROM "test"' \
            + ' LEFT JOIN "ftable" ON ("ftable"."fid" = "test"."lid") GROUP BY "test"."lid"'


## Sanity test

        testQuery "select relation empty object",
            faker.makeSelect( {} ),
            expect

        testQuery "select relation no arguments",
            faker.makeSelect()
            expect

## Array

        testQuery "select relation $all",
            fakerc.makeSelect( some_column: { $all: ['director','producer'] } )
            'SELECT "test".*, ARRAY_AGG(DISTINCT "ftable"."column") "some_column" FROM "test"' \
                + ' LEFT JOIN "ftable" ON ("ftable"."fid" = "test"."lid") GROUP BY "test"."lid"' \
                + ' HAVING ARRAY_AGG(DISTINCT "ftable"."column") @> ARRAY[$1,$2]'
            ['director','producer']

        testQuery "select relation $elemMatch",
            faker.makeSelect( some_column: { $elemMatch: { address: "foo@bar.com", primary: true } } )
            expect + ' HAVING BOOL_OR("ftable"."address" = $1) AND BOOL_OR("ftable"."primary" = $2)'
            ['foo@bar.com','true']

        testQuery "select relation $size array",
            fakerc.makeSelect( some_column: { $size: 10 } )
            expect + ' HAVING COUNT("ftable") = $1'
            [10]

        testQuery "select relation $size ",
            faker.makeSelect( some_column: { $size: 10 } )
            expect + ' HAVING COUNT("ftable") = $1'
            [10]


# Inserts

        expect = 'INSERT INTO "test"'

        testQuery "insert single column",
            fake.makeInsert( id: "abc" ),
            expect + ' ("id") VALUES ($1)',
            ["abc"]

        testQuery "insert multiple columns",
            fake.makeInsert( id: "abc", foo: "def" ),
            expect + ' ("id","foo") VALUES ($1,$2)',
            ["abc","def"]

        testQuery "insert one value into multiple columns",
            fake.makeInsert( id: "abc", foo: "abc" ),
            expect + ' ("id","foo") VALUES ($1,$1)',
            ["abc"]

# Relational inserts

The query shouldn't change from the base case if none of the columns are foreign

        testQuery "insert single value into relational table",
            faker.makeInsert( id: "abc" ),
            expect + ' ("id") VALUES ($1)',
            ["abc"]



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




    Tinytest.add "Postgres - Setup", (test) ->

Set up minimongo and postgres collections in order to
verify that they behave more or less the same

        mong = new Meteor.Collection null

        user = new PgCollection ["test","user"],
            connection: "postgres://localhost/test"
            relations:
                email:
                    foreign_table: ["test","email"]
                    foreign_id: "userid"
                    local_id: "id"


        user.transact ->
            @exec 'CREATE SCHEMA test'

            @exec \
                'CREATE TABLE test.user (
                    id     SERIAL  PRIMARY KEY,
                    name   TEXT    NOT NULL,
                    logins INTEGER DEFAULT 1
                )'

            @exec \
                'CREATE TABLE test.email (
                    address   TEXT    PRIMARY KEY,
                    userid    INTEGER NOT NULL REFERENCES test.user (id),
                    "primary" BOOLEAN NOT NULL DEFAULT false
                )'

            @exec \
                'CREATE TABLE test.role (
                    userid INTEGER REFERENCES test.user (id),
                    role   TEXT NOT NULL
                )'

    Tinytest.add "Postgres - Insert - Happy Path", (test) ->

        user.insert { name: "Alice Cooper" }
        user.insert { name: "Bob Ross",      email: [{ address: "bob@example.com" }] }
        user.insert { name: "Carol Lombard", role:  [{ role: "admin" }] }
        user.insert { name: "Dan Ackroyd",   email: [{ address: "dan@example.com" }], role: [{ role: "editor" }] }
        user.insert { name: "Eve",           email: [{ address: "eve@example.com" }, { address: "eve@foobar.com", primary: true }] }

    Tinytest.add "Postgres - Insert - Sad Path", (test) ->

        test.throws -> user.insert {### empty data ###}
        test.throws -> user.insert { "名字": "王菲" }
        test.throws -> user.insert { name: "Futhark", email: [{ "not-a-column": "fhqwhgads" }] }


    Tinytest.add "Postgres - Update", (test) ->

        throw "not yet implemented"


    Tinytest.add "Postgres - Select", (test) ->

        alice = user.findOne { id: 1 };            test.isNotNull alice
        bob   = user.findOne { name: "Bob Ross" }; test.isNotNull bob
        carol = user.findOne { logins: 3 };        test.isNotNull carol
        dan   = user.find({ id: 4 }).next();       test.isNotNull dan
        eve   = user.find({ id: 5 }).fetch()[0];   test.isNotNull eve



    Tinytest.add "Postgres - Notify", (test) ->

        throw "not yet implemented"


    Tinytest.add "Postgres - Teardown", (test) ->

        user.exec 'DROP SCHEMA test CASCADE'
