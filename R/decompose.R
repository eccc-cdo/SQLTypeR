# ===========================
#' Decompose a dataframe into the following representation:
#'   - a dumbed-down version of the dataframe, where logicals are
#'     coerced to integers, and factors are coerced to strings.
#'   - a meta dataframe containing the information necessary to
#'     recover the logicals and factors in the original dataframe.
#' This operation is reversed by [recomposeDF()].
#'
#' @param df The dataframe to decompose.
#'
#' @export
#' @examples
#' df <- tibble::tibble(
#'   a = factor(c("a", "a", "b"), levels = c("a", "b"), ordered = TRUE),
#'   b = c(TRUE, TRUE, FALSE),
#'   c = c(1,2,3)
#'  )
#'
#' decomposeDF(df)
#'
#' recomposeDF(decomposeDF(df))
#' @importFrom magrittr "%>%"
decomposeDF <- function (df) {
  data <- df %>% purrr::map_dfc(.degenerate_col)

  meta_rows <- df %>% purrr::imap(.meta_row) %>% unlist

  meta <- dplyr::bind_rows(
    tibble::tibble(
      var = character(),
      type = character(),
      details = character()
    ),
    tibble::tribble(
      ~var, ~type, ~details,
      !!!meta_rows
    )
  )

  list(data = data, meta = meta)
}

# Generate a row for the metadata table.
.meta_row <- function(col, col_name) {
  if (is.logical(col))
    c(col_name, "logical", "")
  else if (is.factor(col))
    c(col_name, "factor", jsonlite::toJSON(list(
      ordered = is.ordered(col),
      levels = attributes(col)$levels
    ), auto_unbox = TRUE))
  else
    c(col_name, "other", "")

}

# Dumb-down the column, in preparation for writing to SQL.
.degenerate_col <- function(col) {
  if (is.logical(col))
    as.integer(col)
  else if (is.factor(col))
    as.character(col)
  else
    col
}

# ===========================
#' Recompose
#' Recover the original dataframe from the following decomposed representation:
#'   - a dumbed-down version of the dataframe, where logicals are
#'     coerced to integers, and factors are coerced to strings.
#'   - a meta dataframe containing the information necessary to
#'     recover the logicals and factors in the original dataframe.
#' This operation is reversed by [decomposeDF()].
#'
#' @param decomposed A dataframe in decomposed representation: list(data = data, meta = meta).
#'
#' @export
#' @examples
#' df <- tibble::tibble(
#'   a = factor(c("a", "a", "b"), levels = c("a", "b"), ordered = TRUE),
#'   b = c(TRUE, TRUE, FALSE),
#'   c = c(1,2,3)
#'  )
#'
#' decomposeDF(df)
#'
#' recomposeDF(decomposeDF(df))
#' @importFrom magrittr "%>%"
recomposeDF <- function(decomposed) {
  decomposed %>%
    .intermediate_rep %>%
    purrr::pmap(.recover_col) %>%
    (function(cols) .set_names(cols, decomposed$meta$var)) %>%
    (function(args) do.call(tibble::tibble, args))
}

# Combine data and meta into an intermediate representation as a single tibble
.intermediate_rep <- function(decomposed) {
  dplyr::bind_cols(
    decomposed$meta,
    tibble::tibble(data = decomposed$data %>% purrr::map(~ .x))
  )
}

# Recover a column from a row in the intermediate representation
.recover_col <- function(var, type, details, data) {
  if (type == "logical") {
    as.logical(data)
  } else if (type == "factor") {
    json <- jsonlite::parse_json(details)
    do.call(factor, append(json, list(x = data)))
  } else {
    data
  }
}

# Modified [rlang::set_names()] to handle the empty case, as a no-op.
.set_names <- function(cols, names) {
  if (length(cols) == 0) cols else rlang::set_names(cols, names)
}
