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
    gen.list(gen_col(n), from = 1, to = 5),
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
# Impure Save/Load Tests
test_that("Example from 'sql.R' documentation works.", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  df <- tibble::tibble(
    a = factor(c("a", "a", "b"), levels = c("a", "b"), ordered = TRUE),
    b = c(TRUE, TRUE, FALSE),
    c = c(1,2,3)
   )

  saveDF(con, df, "test_1")

  df2 <- loadDF(con, "test_1")

  DBI::dbDisconnect(con)

  expect_equal(df, df2)
})

test_that("Loading a dataframe from database recovers the saved dataframe identically.",
  forall(gen_df, tests = 100, function(df) {
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

    saveDF(con, df, "test")

    df_loaded <- loadDF(con, "test")

    DBI::dbDisconnect(con)

    expect_equal(df, df_loaded)
}))

test_that("Saving a dataframe does not corrupt other previously saved dataframes.",
  forall(gen.list(gen_df, of = 2), tests = 100, function(dfs) {
    df_a <- dfs[[1]]
    df_b <- dfs[[2]]

    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

    saveDF(con, df_a, "test_a")
    saveDF(con, df_b, "test_b")

    df_a_loaded <- loadDF(con, "test_a")

    DBI::dbDisconnect(con)

    expect_equal(df_a, df_a_loaded)
}))

test_that("Updating a saved dataframe does not corrupt it.",
  forall(gen.list(gen_df, of = 2), tests = 100, function(dfs) {
    df_a <- dfs[[1]]
    df_b <- dfs[[2]]

    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

    saveDF(con, df_a, "test")
    saveDF(con, df_b, "test")

    df_b_loaded <- loadDF(con, "test")

    DBI::dbDisconnect(con)

    expect_equal(df_b, df_b_loaded)
}))
