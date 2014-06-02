
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

    makeSelect = (name, selector, options={}) ->
        columns = (col for col, val of (options.fields ? {}) when val)
        params = []

        where = _.map( selector, (value, name) -> whereClause(name, value, params) ).join " AND "

        query: "SELECT #{columnsToString columns} FROM #{escapeName name}" \
            + if where.length > 0 then " WHERE #{where}" else ""
            + if _.isFinite( options.limit ) then " LIMIT #{options.limit}" else ""
        params: params


Convert Mongo modifiers into SQL `UPDATE` fragments

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

Convert a document/mutator to columns, values, and parameters

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

Convert Mongo document to `INSERT` statement

    makeInsert = (name, document) ->
        {columns, values, params} = cvp document

        query: "INSERT INTO #{escapeName name} (#{columnsToString columns}) VALUES (#{values.join ","})"
        params: params


Convert Mongo selector and mutator to `UPDATE` statement

    makeUpdate = (name, selector, mutator) ->
        {columns, values, params} = cvp mutator, "update"

        where = _.map( selector, (value, name) -> whereClause(name, value, params) ).join " AND "

        query: "UPDATE #{escapeName name} SET (#{columnsToString columns}) = (#{values.join ","})" \
            + if where.length > 0 then " WHERE #{where}" else ""
        params: params


Convert Mongo selector to `DELETE` statement

    makeDelete = (name, selector) ->
        params = []
        where = _.map( selector, (value, name) -> whereClause(name, value, params) ).join " AND "

        query: "DELETE FROM #{escapeName name}" \
            + if where.length > 0 then " WHERE #{where}" else ""
        params: params


Object that is thrown to trigger a rollback

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


Set up endpoints for the client to hit

    setupMethods = (collection, methods={}) ->
        pfx = "/#{collection.name}\x2f"
        m = {}

        _.each ["insert", "update", "remove"], (method) ->
            m[pfx + method] = ->
                validateMethodName = "_validate" + method.charAt(0).toUpperCase() + method.slice(1)
                collection[validateMethodName].apply collection, arguments
                (methods[method] ? collection[method]).apply collection, arguments

        Meteor.methods m


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
            for row in @rows
                for fn in PgCursor._listeners[@name].added
                    fn? row.id, row

        observe: (callbacks) ->
            @observeChanges callbacks

        observeChanges: (callbacks) ->
            self = @
            ordered = LocalCollection._observeChangesCallbacksAreOrdered callbacks
            unless PgCursor._listeners[@name]
                listeners = PgCursor._listeners[@name] = {}
                @collection.listen ([schema,table,action,fields,pkeys]) ->

                    if _.size(pkeys) == 1
                        pkeys = _.values(pkeys)[0]

                    fns = switch action
                        when "I" then listeners.added
                        when "U" then listeners.changed
                        when "D" then listeners.removed

                    for fn in fns
                        fn? pkeys, fields

                    undefined

            else
                listeners = PgCursor._listeners[@name]

            for fname, fn of callbacks
                if listeners[fname]
                    listeners[fname].push fn
                else
                    listeners[fname] = [fn]

            stop: ->
                self.collection.unlisten
                for fname, fn of callbacks
                    if listeners[fname]
                        listeners[fname] = _.without listeners[fname], fn
                undefined


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

        constructor: (@name, options={}) ->
            @listen_client = {}
            @_validators =
                insert: { allow: [], deny: [] }
                update: { allow: [], deny: [] }
                remove: { allow: [], deny: [] }
            @config = _.extend {}, PgCollection._config, options
            setupMethods @, options.methods


Simulate Mongo's `find` method

        find: (criteria, options) ->
            {query, params} = makeSelect @name, criteria, options
            new PgCursor @, @exec query, params

        findOne: (criteria, options) ->
            @find( criteria, _.extend {limit:1}, options ).toArray()[0]

Check permissions before allowing the client to insert/update/delete

        _validateInsert: (userId, document) ->
            if _.any( self._validators.insert.deny, (validator) -> validator userId, doc )
                throw new Meteor.Error 403, "Access denied"
            if _.all( self._validators.insert.allow, (validator) -> not validator userId, doc )
                throw new Meteor.Error 403, "Access denied"

        _modifiedFields: (mutator) ->
            fields = []
            _.each mutator, (params, op) ->
                _.each _.keys params, (field) ->
                    unless field.indexOf '.' is -1
                        field = field.substring(0, field.indexOf '.')
                    unless _.contains fields, field
                        fields.push( field )
            fields

        _validateUpdate: (userId, document, mutator, options) ->
            fields = @_modifiedFields mutator
            if _.any( self._validators.upate.deny, (validator) -> validator userId, doc, fields, mutator )
                throw new Meteor.Error 403, "Access denied"
            if _.all( self._validators.update.allow, (validator) -> not validator userId, doc, fields, mutator )
                throw new Meteor.Error 403, "Access denied"


        _validateRemove: (userId, document) ->
            if _.any( self._validators.remove.deny, (validator) -> validator userId, doc )
                throw new Meteor.Error 403, "Access denied"
            if _.all( self._validators.remove.allow, (validator) -> not validator userId, doc )
                throw new Meteor.Error 403, "Access denied"


        _validateExec: (userId, statement) ->
            if _.any( self._validators.exec.deny, (validator) -> validator userId, statement )
                throw new Meteor.Error 403, "Access denied"
            if _.all( self._validators.exec.allow, (validator) -> not validator userId, statement )
                throw new Meteor.Error 403, "Access denied"



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

        update: (selector, mutator, options={}) ->
            {query, params} = makeUpdate @name, selector, mutator
            @exec query, params

Simulate Mongo's `remove` method

        remove: (selector, options={}) ->
            {query, params} = makeDelete @name, selector
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

            unless @listen_client[channel]
                @listen_client[channel] = client = new pg.Client @config.connection
                client.connect()
                client.query "LISTEN #{escapeName channel}"
                client.on "notification", (msg) ->
                    data = JSON.parse msg.payload
                    Fiber( -> fn data ).run()

        unlisten: (channel) ->
            channel ?= @name
            @listen_client[channel]?.query "UNLISTEN #{escapeName channel}"
            @listen_client[channel]?.end()
            delete @listen_client[channel]



        allow: -> Meteor.Collection::allow.apply @, arguments

        deny: -> Meteor.Collection::deny.apply @, arguments
