test_that("check postgres source", {
  skip_on_cran()

  # local connection works
  expect_no_error(con <- localPostgres())
  expect_true(inherits(con, "PqConnection"))

  # create postgres source
  expect_no_error(src <- postgresSource(con = con))

  # copy cdm to postgres
  cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed")
  expect_no_error(pq_cdm <- insertCdmTo(cdm = cdm, to = src))

  # drop all created tables
  ls <- listTables(src = src, type = "cdm")
  expect_no_error(dropTable(src = src, type = "cdm", name = ls))
  expect_true(length(listTables(src = src, type = "cdm")) == 0)
  ls <- listTables(src = src, type = "write")
  expect_no_error(dropTable(src = src, type = "write", name = ls))
  expect_true(length(listTables(src = src, type = "write")) == 0)
  ls <- listTables(src = src, type = "achilles")
  expect_no_error(dropTable(src = src, type = "achilles", name = ls))
  expect_true(length(listTables(src = src, type = "achilles")) == 0)

  # disconnect
  expect_no_error(cdmDisconnect(cdm = pq_cdm))
})
