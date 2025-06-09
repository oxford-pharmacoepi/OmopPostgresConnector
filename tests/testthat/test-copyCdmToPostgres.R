test_that("copyCdmToPostgres", {
  skip_on_cran()
  cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed")
  expect_no_error(pq_cdm <- copyCdmToPostgres(
    cdm = cdm,
    con = localPostgres(),
    cdmPrefix = omopgenerics::tmpPrefix(),
    writePrefix = omopgenerics::tmpPrefix(),
    achillesPrefix = omopgenerics::tmpPrefix()
  ))

  # disconnect
  expect_no_error(dropCdm(cdm = pq_cdm))
})
