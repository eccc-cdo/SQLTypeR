# ====================================
# Error Triggers for saveDF/loadDF
test_that("Connection closed error triggers for saveDF.", {
  df <- tibble::tibble(a = c(1))

  test <- function(){
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    DBI::dbDisconnect(con)
    saveDF(con, df, "test")
  }

  expect_error(test(), 'Database connection is closed.')
})

test_that("Read-only connection error triggers for saveDF.", {
  df <- tibble::tibble(a = c(1))

  test <- function(){
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:",
                          flags = RSQLite::SQLITE_RO)
    saveDF(con, df, "test")
    DBI::dbDisconnect(con)
  }

  expect_error(test(), 'attempt to write a readonly database')
})

test_that("Empty dataframe error triggers for saveDF.", {
  df <- tibble::tibble()

  test <- function(){
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    saveDF(con, df, "test")
    DBI::dbDisconnect(con)
  }

  expect_error(test(), 'Dataframe must have at least one column to write it to database.')
})

test_that("Connection closed error triggers for loadDF.", {
  test <- function(){
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    DBI::dbDisconnect(con)
    loadDF(con, "test")
  }

  expect_error(test(), 'Database connection is closed.')
})

test_that("Metadata non-existence error triggers for loadDF.", {
  df <- tibble::tibble(a = c(1))
  decomposed <- decomposeDF(df)
  data <- decomposed$data
  meta <- decomposed$meta %>%
    dplyr::mutate(table = "test")

  test <- function(){
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    DBI::dbCreateTable(con, "test", data)
    invisible(DBI::dbAppendTable(con, "test", data))
    loadDF(con, "test")
    DBI::dbDisconnect(con)
  }

  expect_error(test(), 'Metadata table __rtypes does not exist in the database.')
})

test_that("Table non-existence error triggers for loadDF.", {
  df <- tibble::tibble(a = c(1))
  decomposed <- decomposeDF(df)
  data <- decomposed$data
  meta <- decomposed$meta %>%
    dplyr::mutate(table = "test")

  test <- function(){
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    DBI::dbCreateTable(con, '__types', meta)
    invisible(DBI::dbAppendTable(con, '__types', meta))
    loadDF(con, "test")
    DBI::dbDisconnect(con)
  }

  expect_error(test(), 'Table does not exist in the database.')
})
