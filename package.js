Package.describe({
    summary: "PostgreSQL for Meteor"
});

Npm.depends({ "pg.js": "3.1.0" });

Package.on_use(function(api) {
    api.use( ['underscore', 'coffeescript', 'npm', 'minimongo'], 'server' );
    api.export( ['PgCollection'] );
    api.add_files( ['postgres_server.litcoffee'], 'server');
});

Package.on_test(function(api) {
    api.use( ['postgres', 'tinytest', 'test-helpers', 'coffeescript'], 'server' );
    api.add_files( ['postgres_server_tests.litcoffee'], 'server' );
});
