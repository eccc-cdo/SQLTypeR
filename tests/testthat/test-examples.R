# ====================================
# Documentation Examples
test_that("Example from 'decompose.R' documentation works.", {
  df <- tibble::tibble(a = factor(c("a","a","b"), levels = c("a","b"), ordered = TRUE))
  expect_equal(df, recomposeDF(decomposeDF(df)))
})

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
