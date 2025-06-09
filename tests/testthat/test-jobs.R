test_that("job management", {
  skip_on_cran()
  con <- localPostgres()
  cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed")
  pcdm <- copyCdmToPostgres(cdm = cdm, con = con, cdmPrefix = "job_c_", writePrefix = "job_w_")

  expect_no_error(jb <- getJobs(pcdm))
  expect_true(inherits(jb, "tbl"))
  expect_true(nrow(jb) > 0)

  user <- Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector")
  expect_no_error(getJobs(src = pcdm, user = user))

  expect_message(expect_message(cancelJob(pcdm, 123456789)))

  dropCdm(pcdm)
})
