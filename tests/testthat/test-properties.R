library(hedgehog)

# ====================================
# Generators
# Generate 1 to 26 factor levels as unique single characters.
gen_factor_levels <- gen.with(
  gen.int(26),
  function(m) letters[1:m]
)

# Generate a random-leveled ordered/unordered factor column with (n >= 0) values.
gen_factor <- function(n) gen.and_then(
  gen_factor_levels,
  function (levels) gen.with(
    list(
      x = if (n == 0) gen.pure(character()) else gen.c(gen.element(levels), of = n),
      levels = levels,
      ordered = gen.element(c(FALSE, TRUE))
    ),
    function(args) do.call(factor, args)
  )
)

# Generate a random-typed column with (n >= 0) values.
gen_col <- function(n) gen.and_then(
  gen.element(c("factor","logical","integer","character")),
  function(type) {
    if (type == "factor")
      gen_factor(n)
    else if (type == "logical")
      if (n == 0) gen.pure(logical()) else gen.c(gen.element(c(FALSE, TRUE)), of = n)
    else if (type == "character")
      if (n == 0) gen.pure(character()) else gen.c(gen.element(letters), of = n)
    else if (type == "integer")
      if (n == 0) gen.pure(integer()) else gen.c(gen.int(10), of = n)
  }
)

# Generate a tibble with 0 to 5 rows, with 0 and 5 random columns.
gen_df <- gen.and_then(
  gen.int(5 + 1),
  function(n) gen.with(
    gen.list(gen_col(n - 1), from = 0, to = 5),
    function(cols) {
      m <- length(cols)
      named_cols <- if (m == 0) cols else rlang::set_names(cols, letters[1:m])
      do.call(dplyr::tibble, named_cols)
    }
  )
)

# ====================================
# Pure Decompose/Recompose Tests
test_that("Example from 'decompose.R' documentation works.", {
  df <- tibble::tibble(a = factor(c("a","a","b"), levels = c("a","b"), ordered = TRUE))
  expect_equal(df, recomposeDF(decomposeDF(df)))
})

test_that("Recompose is the inverse of decompose.",
  forall(gen_df, test = 100, function(df) {
    expect_equal(df, recomposeDF(decomposeDF(df)))
}))

# ====================================
# Impure Save/Load Error Triggers
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

# ====================================
# Impure Save/Load Tests
test_that("Example from 'sql.R' documentation works.", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  df <- tibble::tibble(
    a = factor(c("a", "a", "b"), levels = c("a", "b"), ordered = TRUE),
    b = c(TRUE, TRUE, FALSE),
    c = c(1,2,3)
   )

  saveDF(con, df, "test")

  df2 <- loadDF(con, "test")

  DBI::dbDisconnect(con)

  expect_equal(df, df2)
})

test_that("Loading a dataframe from database recovers the saved dataframe identically.",
  forall(gen_df, tests = 100, function(df) {
    test <- function() {
      con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

      saveDF(con, df, "test")

      df_loaded <- loadDF(con, "test")

      DBI::dbDisconnect(con)

      df_loaded
    }

    if (length(df) == 0)
      expect_error(test(), 'Dataframe must have at least one column to write it to database.')
    else
      expect_equal(df, test())

}))

test_that("Saving a dataframe does not corrupt other previously saved dataframes.",
  forall(gen.list(gen_df, of = 2), tests = 100, function(dfs) {
    df_a <- dfs[[1]]
    df_b <- dfs[[2]]

    test <- function() {
      con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

      saveDF(con, df_a, "test_a")
      saveDF(con, df_b, "test_b")

      df_a_loaded <- loadDF(con, "test_a")

      DBI::dbDisconnect(con)

      df_a_loaded
    }

    if (length(df_a) == 0 || length(df_b) == 0)
      expect_error(test(), 'Dataframe must have at least one column to write it to database.')
    else
      expect_equal(df_a, test())
}))

test_that("Updating a saved dataframe does not corrupt it.",
  forall(gen.list(gen_df, of = 2), tests = 100, function(dfs) {
    df_a <- dfs[[1]]
    df_b <- dfs[[2]]

    test <- function() {
      con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

      saveDF(con, df_a, "test")
      saveDF(con, df_b, "test")

      df_b_loaded <- loadDF(con, "test")

      DBI::dbDisconnect(con)

      df_b_loaded
    }

    if (length(df_a) == 0 || length(df_b) == 0)
      expect_error(test(), 'Dataframe must have at least one column to write it to database.')
    else
      expect_equal(df_b, test())
}))

