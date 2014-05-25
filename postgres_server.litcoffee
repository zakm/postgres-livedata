
    class PgCollection

        constructor: (@name, options) ->
            @config = _.extend {}, PgCollection._config, options

        @_config:
            connection: null

        @config: (options) =>
            _.extend @_config, options

        @escapeName: (name) ->
            '"' + name.replace(/"/g, '""') + '"'

        @escapeValue: (val) ->
            if typeof val is "number"
                val
            else
                "'#{val.replace /'/g, "''"}'"

        @columnsToString: (columns) =>
            if columns.length > 0
                _.map( columns, @escapeName ).join ","
            else
                "*"

        @operatorClause: (name, val) =>
            ops =
                $gt: ">"
                $gte: ">="
                $lt: "<"
                $lte: "<="
                $ne: "!="
            for mop,pop of ops
                if val[mop]?
                    return "#{@escapeName name} #{pop} #{@escapeValue val[mop]}"

            if vals = val.$in?
                vals = _.map( val.$in, @escapeValue ).join ","
                return "#{@escapeName name} IN (#{vals})"

            if val.$nin?
                vals = _.map( val.$nin, @escapeValue ).join ","
                return "#{@escapeName name} NOT IN (#{vals})"

        @simpleSelector: (selector) =>
            "(" + _.map( selector, @whereClause ).join(" AND ") + ")"

        @whereClause: (value, name) =>
            self = @
            if name is "$or"
                _.map( value, @simpleSelector ).join(" OR ")
            else if name is "$and"
                _.map( value, @simpleSelector ).join(" AND ")
            else if name is "$not"
                "NOT (" + _.map( value, @whereClause ).join(" AND ") + ")"
            else if name is "$nor"
                "NOT " + _.map( value, @simpleSelector ).join(" OR ")
            else
                switch typeof value
                    when "string", "number"
                        "#{@escapeName name} = #{@escapeValue value}"
                    else
                        @operatorClause name, value

        @selectorToQuery: (name, selector, projection={}) =>
            columns = (col for col, val of projection when val)

            where = _.map( selector, @whereClause ).join " AND "

            "SELECT #{@columnsToString columns} FROM #{@escapeName name}" \
                + if where.length > 0 then " WHERE #{where}" else ""


    Meteor.methods
        query: (name, selector, projection) ->
            PgCollection.selectorToQuery name, selector, projection
