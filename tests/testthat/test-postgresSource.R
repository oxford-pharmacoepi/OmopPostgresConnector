test_that("check postgres source", {
  skip_on_cran()

  # local connection works
  expect_no_error(con <- localPostgres())
  expect_true(inherits(con, "PqConnection"))

  # create postgres source
  expect_no_error(src <- postgresSource(con = con))
})
