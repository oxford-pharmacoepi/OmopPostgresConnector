test_that("create schema", {
  con <- testCon()
  expect_false(schemaExists(con = con, schema = "test_schema"))
  expect_no_error(createSchema(con = con, schema = "test_schema"))
  expect_true(schemaExists(con = con, schema = "test_schema"))
  expect_no_error(deleteSchema(con = con, schema = "test_schema"))
  expect_false(schemaExists(con = con, schema = "test_schema"))
  DBI::dbDisconnect(conn = con)
})
