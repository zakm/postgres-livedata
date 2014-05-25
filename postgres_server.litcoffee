
    pg = Npm.require "pg.js"


## Helper functions

Escape Postgres identifiers

    escapeName = (name) ->
        '"' + name.replace(/"/g, '""') + '"'


Convert a list of column names to a string

    columnsToString = (columns) ->
        if columns.length > 0
            _.map( columns, escapeName ).join ","
        else
            "*"


Build up list of parameters for paramaterized queries

    nextParam = (val, params) ->
        if (idx = params.indexOf val) >= 0
            "$#{idx + 1}"
        else
            params.push(val)
            "$#{params.length}"


Convert Mongo operators to Postgres operators

    operatorClause = (name, val, params) ->
        ops =
            $gt: ">"
            $gte: ">="
            $lt: "<"
            $lte: "<="
            $ne: "!="
        for mop,pop of ops
            if val[mop]?
                return "#{escapeName name} #{pop} #{nextParam val[mop] params}"

        if vals = val.$in?
            vals = _.map( val.$in, (v) -> nextParam(v, params) ).join ","
            return "#{escapeName name} IN (#{vals})"

        if val.$nin?
            vals = _.map( val.$nin, (v) -> nextParam(v, params) ).join ","
            return "#{escapeName name} NOT IN (#{vals})"


Helper for whereClause

    simpleSelector = (selector, params) ->
        "(" + _.map( selector, (value, name) -> whereClause(name, value, params) ).join(" AND ") + ")"


Convert Mongo selector to Postgres `WHERE` clause

    whereClause = (name, value, params) ->
        if name is "$or"
            _.map( value, (selector) -> simpleSelector(selector, params) ).join(" OR ")
        else if name is "$and"
            _.map( value, (selector) -> simpleSelector(selector, params) ).join(" AND ")
        else if name is "$not"
            "NOT (" + _.map( value, (value, name) -> whereClause(name, value, params) ).join(" AND ") + ")"
        else if name is "$nor"
            "NOT " + _.map( value, (selector) -> simpleSelector(selector, params) ).join(" OR ")
        else
            switch typeof value
                when "string", "number"
                    "#{escapeName name} = #{nextParam value, params}"
                else
                    operatorClause name, value, params


Convert Mongo query to SQL `SELECT` statement

    makeSelect = (name, selector, projection={}) ->
        columns = (col for col, val of projection when val)
        params = []

        where = _.map( selector, (value, name) -> whereClause(name, value, params) ).join " AND "

        query: "SELECT #{columnsToString columns} FROM #{escapeName name}" \
            + if where.length > 0 then " WHERE #{where}" else ""
        params: params

Convert a document into columns, values, and parameters

    cvp = (document) ->
        columns = []
        values = []
        params = []

        for col, val of document
            columns.push col
            values.push nextParam val, params

        columns: columns
        values: values
        params: params

Convert Mongo document into `INSERT` statement

    makeInsert = (name, document) ->
        {columns, values, params} = cvp document

        query: "INSERT INTO #{escapeName name} (#{columnsToString columns}) VALUES (#{values.join ","})"
        params: params

    makeUpdate = (name, selector, document) ->
        {columns, values, params} = cvp document

        where = _.map( selector, (value, name) -> whereClause(name, value, params) ).join " AND "

        query: "UPDATE #{escapeName name} SET (#{columnsToString columns}) = (#{values.join ","})" \
            + if where.length > 0 then " WHERE #{where}" else ""
        params: params


A unique object that can be thrown to trigger a rollback

    doRollback = {doRollback:true}


Called in `PgCollection::transact` to rollback changes

    rollback = (client, release) ->
        client.query "ROLLBACK", (err) ->
            release err


Provides `this` in `PgCollection::transact`

    ctx = (client, release) ->
        rollback: -> throw doRollback
        exec: (query, params...) ->
            Async.wrap(client.query) query, params



# PgCollection

Should act as a drop-in replacement for Meteor.Collection, but is backed by
PostgreSQL (!!)

    class PgCollection


## Class methods

Global configuration

        @_config:
            connection: null

        @config: (options) =>
            _.extend @_config, options


## Instance methods

Allow instances to have custom configuration, but default to global config

        constructor: (@name, options) ->
            @config = _.extend {}, PgCollection._config, options


Simulate Mongo's `find` method

        find: (criteria, projection) ->
            {query, params} = makeSelect @name, criteria, projection
            @exec query, params


Simulate Mongo's `insert` method

        insert: (document, options={}) ->
            self = @
            if _.isArray document
                if options.atomic
                    @transact ->
                        for doc in document
                            {query, params} = makeInsert self.name, doc
                            @exec query, params
                else
                    for doc in document
                        {query, params} = makeInsert @name, doc
                        @exec query, params
            else
                {query, params} = makeInsert @name, document
                @exec query, params


Simulate Mongo's `update` method

        update: (selector, document, options) ->
            {query, params} = makeUpdate @name, selector, document
            @exec query, params


Connect to the postgres server and execute `fn`

        connect: (fn) ->
            self = @
            {error, result} = Async.runSync (done) ->
                pg.connect self.config.connection, (err, client, release) ->
                    fn client, release, done
            throw error if error
            result


Execute a parameterized query

        exec: (query, params) ->
            console.log "exec", query, params
            @connect (client, release, done) ->
                client.query query, params, (err, result) ->
                    release()
                    done err, result

Execute a function within a transaction. Either all or none of the `@exec()` calls will
actually take effect. `@rollback()` to rollback your changes.

        transact: (fn) =>
            @connect (client, release, done) ->
                client.query "BEGIN", (err) ->
                    return rollback client, release if err
                    try
                        result = fn.call ctx(client, release)
                        client.query "COMMIT", release
                        done null, result
                    catch e
                        rollback client, release
                        done if e is doRollback then null else e


Interactive testing

    Meteor.methods
        insertQuery: (name, document) ->
            makeInsert name, document

        find: (name, selector, projection) ->
            coll = new PgCollection name,
                connection: "postgres://zakm@localhost\x2f"

            try
                coll.find selector, projection
            catch e
                throw new Meteor.Error 500, JSON.stringify e
