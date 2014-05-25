Package.describe({
    summary: "PostgreSQL for Meteor"
});

Npm.depends({ "pg.js": "3.1.0" });

Package.on_use(function(api) {
    api.use( ['underscore','coffeescript','npm'], ['client','server'] );
    api.export( ['PgCollection'] );

    api.add_files( ['postgres_client.js'], 'client');
    api.add_files( ['postgres_server.litcoffee'], 'server');
});

Package.on_test(function(api) {
    api.use( ['postgres', 'tinytest', 'test-helpers', 'coffeescript', 'npm'] );
    api.add_files( ['postgres_client_tests.js'], 'client' );
    api.add_files( ['postgres_server_tests.litcoffee'], 'server' );
});
