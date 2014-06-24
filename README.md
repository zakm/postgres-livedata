## About

A work in progress, but already useful if you keep writes to the database on the server-side.

At present, I'm fairly new to Meteor (especially its internals) so I'd be happy to consider your
suggestions and/or pull requests.

## Usage

Import `notify.sql` and ensure changes to your table will trigger notifications
(the second paramter of `notify_table` is the channel name, but it must be the
same as the table name for now).

```
db=# \i notify.sql
db=# CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT, author TEXT, ...);
db=# SELECT notify.notify_table('posts','posts');
```

On the server, set up the default connection and create a new `PgCollection`
(which is loosely an interface to a single pg table)


```js
if (Meteor.isServer) {
    PgCollection.config({
        connection: "postgres://user:pass@localhost/database"
    });

    Posts = new PgCollection("posts");

    Meteor.publish("posts", function() {
        return Posts.find();
    });
}
```

On the client, you can use regular old minimongo collections.

```js
if (Meteor.isClient) {
    Posts = new Meteor.Collection("posts");
    Meteor.subscribe("posts");

    Template.page.posts = function() {
        return Posts.find();
    };
}
```

Reactive templates don't look any different.

```html
<template name="page">
    <table>
        {{#each posts}}
        <tr>
            <td>{{title}}</td>
            <td>{{author}}</td>
            ...
        </tr>
        {{/each}}
    </table>
</template>
```

## TODO

- Client-side `insert`s work, but `update`s are broken, `remove`s haven't been tested anywhere
- Update the tests to cover recent changes
- Handle tables with multiple primary keys
- Reconcile Meteor/Mongo's `_id`s with Postgres' primary keys
- Have a better way to describe relations between table that make up a "document", maybe using (simple schema)[https://github.com/aldeed/meteor-simple-schema]?
