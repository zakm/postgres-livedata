
    pg = Npm.require "pg.js"
    Fiber = Npm.require "fibers"


## Helper functions

Escape Postgres identifiers

    escapeName = (name) ->
        if _.isArray name
            _.map(name, escapeName).join "."
        else
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


    updates = (col, val, columns, values, params) ->
        switch col
            when "$inc"
                for c, v of val
                    columns.push c
                    next = nextParam v, params
                    values.push "#{escapeName c} + #{next}"

            when "$mul"
                for c, v of val
                    columns.push c
                    next = nextParam v, params
                    values.push "#{escapeName c} * #{next}"

            when "$set"
                for c, v of val
                    columns.push c
                    values.push nextParam v, params

            when "$unset"
                for c, v of val
                    columns.push c
                    values.push "NULL"

            when "$min"
                for c, v of val
                    columns.push c
                    next = nextParam v, params
                    values.push "LEAST(#{escapeName c},#{next})"

            when "$max"
                for c, v of val
                    columns.push c
                    next = nextParam v, params
                    values.push "GREATEST(#{escapeName c},#{next})"

            when "$currentDate"
                for c, v of val
                    columns.push c
                    values.push "NOW()"

            else
                columns.push col
                values.push nextParam val, params

Convert a document into columns, values, and parameters

    cvp = (document, update) ->
        columns = []
        values = []
        params = []

        if update?
            for col, val of document
                updates col, val, columns, values, params
        else
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
        {columns, values, params} = cvp document, "update"

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

# PgCursor

Should behave like a Mongo cursor

    class PgCursor

        @_listeners: {}

        constructor: (collection, result) ->
            @collection = collection
            @name = collection.name
            @_next = 0
            @rows = result.rows

        _publishCursor: (sub) ->
            Meteor.Collection._publishCursor @, sub, @name

        observe: (callbacks) ->
            LocalCollection._observeFromObserveChanges @, callbacks

        observeChanges: (callbacks) ->
            self = @
            ordered = LocalCollection._observeChangesCallbacksAreOrdered callbacks
            if not PgCursor._listeners[@name]
                listeners = PgCursor._listeners[@name] = {}
                @collection.listen ([schema,table,action,fields,pkeys]) ->
                    if _.size(pkeys) == 1
                        pkeys = _.sample(pkeys)

                    fns = switch action
                        when "I" then listeners.added.concat listeners.addedBefore
                        when "U" then listeners.changed
                        when "D" then listeners.removed

                    for fn in fns
                        fn pkeys, fields

            else
                listeners = PgCursor._listeners[@name]

            for fname, fn of callbacks
                if listeners[fname]
                    listeners[fname].push fn
                else
                    listeners[fname] = [fn]


        count: ->
            @rows.length

        forEach: (fn) ->
            for row in rows
                fn row
            undefined

        hasNext: ->
            @_next < @rows.length

        map: (fn) ->
            _.map @rows, fn

        next: ->
            @rows[@_next++] if @hasNext

        objsLeftInBatch: ->
            @rows.length - @_next

        size: ->
            @rows.length - @_next

        skip: (n) ->
            @_next += n

        sort: (sort) ->
            @rows.sort (a,b) ->
                for field, ord of sort
                    if a[field] < b[field]
                        return ord
                    else if a[field] > b[field]
                        return -ord

                return 0

        toArray: ->
            @rows


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
            new PgCursor @, @exec query, params


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
            console.log query, params
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


Listen for notifications on the given channel and call `fn` when received
If only a callback is passed, then default the channel to the collection name

        listen: (channel, fn) ->
            if _.isFunction channel
                fn = channel
                channel = @name
            client = new pg.Client @config.connection
            client.connect()
            client.query "LISTEN #{escapeName channel}"
            client.on "notification", (msg) ->
                data = JSON.parse msg.payload
                Fiber( -> fn data ).run()


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
