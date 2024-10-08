% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/load_test.R
\name{load_test}
\alias{load_test}
\title{load tests}
\usage{
load_test(
  pddir,
  dbf,
  ow = F,
  db = F,
  tab_name = "test",
  add = F,
  selvars2 = c("patid", "eventdate", "sysdate", "constype", "consid", "medcode", "sctid",
    "sctdescid", "sctexpression", "sctmaptype", "sctmapversion", "sctisindicative",
    "sctisassured", "staffid", "enttype", "data1", "data2", "data3", "data4", "data5",
    "data6", "data7", "data8")
)
}
\arguments{
\item{batch}{the name of the batch subdirectory}

\item{datadir}{the name of the data directory}

\item{bpp}{BiocParallal Multicore Parameters}
}
\description{
get the drugissue records and convert some fields to useful field types
}
