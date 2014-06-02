CREATE EXTENSION IF NOT EXISTS hstore;

CREATE SCHEMA IF NOT EXISTS notify;
REVOKE ALL ON SCHEMA notify FROM public;

CREATE OR REPLACE FUNCTION notify.trigger_func() RETURNS TRIGGER AS $body$
DECLARE
    json_string TEXT := '[' || to_json(TG_TABLE_SCHEMA::text) || ',' || to_json(TG_TABLE_NAME::text) || ',';
BEGIN
    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
        PERFORM pg_notify( TG_ARGV[0]::text, json_string || '"U",' || hstore_to_json_loose( hstore(NEW.*) - hstore(OLD.*) )
                || ',' || hstore_to_json_loose( slice( hstore(NEW.*), TG_ARGV[1]::text[] ) ) || ']' );
    ELSIF (TG_OP = 'DELETE' AND TG_LEVEL = 'ROW') THEN
        PERFORM pg_notify( TG_ARGV[0]::text, json_string || '"D",' || hstore_to_json_loose( hstore(OLD.*) )
                || ',' || hstore_to_json_loose( slice( hstore(OLD.*), TG_argv[1]::text[] ) ) || ']' );
    ELSIF (TG_OP = 'INSERT' AND TG_LEVEL = 'ROW') THEN
        PERFORM pg_notify( TG_ARGV[0]::text, json_string || '"I",' || hstore_to_json_loose( hstore(NEW.*) )
                || ',' || hstore_to_json_loose( slice( hstore(NEW.*), TG_ARGV[1]::text[] ) ) || ']' );
    ELSIF (TG_OP = 'TRUNCATE' AND TG_LEVEL = 'STATEMENT') THEN
        PERFORM pg_notify( TG_ARGV[0]::text, json_string || '"T"]' );
    END IF;
RETURN NEW;
END;
$body$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notify.notify_table(target_table regclass, channel text) RETURNS VOID AS $body$
DECLARE
    pkeys text[];
BEGIN
    EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident( 'notify_' || channel || '_row' ) || ' ON ' || target_table;
    EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident( 'notify_' || channel || '_stm' ) || ' ON ' || target_table;

    SELECT array_agg(pg_attribute.attname) INTO pkeys FROM pg_index, pg_class, pg_attribute WHERE pg_class.oid = target_table AND indrelid = pg_class.oid AND pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_index.indkey) AND indisprimary;

    EXECUTE 'CREATE TRIGGER ' || quote_ident( 'notify_' || channel || '_row' ) || ' AFTER INSERT OR UPDATE OR DELETE ON ' || target_table
            || ' FOR EACH ROW EXECUTE PROCEDURE notify.trigger_func(' || quote_ident( channel ) || ',' || quote_literal( pkeys ) || ')';
    EXECUTE 'CREATE TRIGGER ' || quote_ident( 'notify_' || channel || '_stm' ) || ' AFTER TRUNCATE ON ' || target_table
            || ' FOR EACH STATEMENT EXECUTE PROCEDURE notify.trigger_func(' || quote_ident( channel ) || ',' || quote_literal( pkeys ) || ')';
END;
$body$
LANGUAGE plpgsql;
