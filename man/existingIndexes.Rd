% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/indexes.R
\name{existingIndexes}
\alias{existingIndexes}
\title{Retrieve the indexes that exist in database.}
\usage{
existingIndexes(x, schema = NULL, tableName = NULL)
}
\arguments{
\item{x}{A PqConnection, cdm_reference, cdm_table or pq_cdm object.}

\item{schema}{schema}

\item{tableName}{tableName}
}
\value{
A tibble object with the existing indexes.
}
\description{
Retrieve the indexes that exist in database.
}
