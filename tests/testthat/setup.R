
dropCdm <- function(cdm) {
  src <- omopgenerics::cdmSource(x = cdm)

  # get tables
  x <- cdmTableClasses(cdm = cdm)

  # drop cdm tables
  dropTable(src = src, type = "cdm", name = x$omop_tables)

  # drop write tables
  dropTable(src = src, type = "write", name = c(x$cohort_tables, x$other_tables))

  # drop achilles tables
  dropTable(src = src, type = "achilles", name = x$achilles_tables)

  # disconnect
  cdmDisconnect(cdm = cdm)
}
deleteAllTables <- function() {
  con <- localPostgres()
  DBI::dbExecute(
    conn = con,
    statement = "DO
    $$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        LOOP
            EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;', r.schemaname, r.tablename);
        END LOOP;
    END
    $$;"
  )
  DBI::dbDisconnect(conn = con)
}
