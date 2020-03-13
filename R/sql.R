# ===========================
#' Save
#' Save a dataframe to a database, storing it in decomposed form:
#'   - a named table with a dumbed-down version of the dataframe, where logicals are
#'     coerced to integers, and factors are coerced to strings.
#'   - a metadata table called `__types`, with the information necessary to
#'     recover the logicals and factors in the original dataframe.
#' Additional arguments are passed to the database write call for the data table.
#' The dataframe can be retrieved from the database using [loadDF()].
#'
#' @param con An SQL database connection (generated via [DBI::dbConnect()]).
#' @param df The dataframe to save.
#' @param name The table name to use in the database.
#' @param overwrite Should the dataframe overwrite the existing content under that table name?
#' @param ... Additional arguments to pass to the database write call for the dataframe.
#'
#' @seealso Pure functions to decompose/recompose dataframes: [decomposeDF()] [recomposeDF()]
#' @export
#' @examples
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#'
#' df <- tibble::tibble(
#'   a = factor(c("a", "a", "b"), levels = c("a", "b"), ordered = TRUE),
#'   b = c(TRUE, TRUE, FALSE),
#'   c = c(1,2,3)
#'  )
#'
#' saveDF(con, df, "test_1")
#'
#' loadDF(con, "test_1")
#'
#' DBI::dbDisconnect(con)
#' @importFrom magrittr "%>%"
saveDF <- function(con, df, name, overwrite = TRUE, ...) {
  # Show stoppers
  if (!DBI::dbIsValid(con))
    stop('Database connection is closed.')
  if (length(df) == 0)
    stop('Dataframe must have at least one column to write it to database.')

  # Allow me to explain.
  # DBI::dbIsReadOnly appears to be broken: it does not detect a read-only SQLite connection.
  # So, as a workaround, we need to trigger the underlying SQLite error,
  # **by attempting to write to the database.**
  # Therefore, we will forge on ahead to the DBI transaction, below,
  # which will rollback all database changes if the connection is read-only.

  # Decompose df into the dumbed-down table and meta records
  decomposed <- decomposeDF(df)
  data <- decomposed$data
  meta <- decomposed$meta %>%
    dplyr::mutate(table = name)

  # Begin database operations
  DBI::dbWithTransaction(con, {

    if (!DBI::dbExistsTable(con, '__types')) {
      # First-time setup
      DBI::dbCreateTable(con, '__types', meta)
      invisible(DBI::dbAppendTable(con, '__types', meta))
    } else {
      # Metadata for other tables should be maintained,
      # but remove all metadata associated with the current dataframe name.
      other_meta <- dplyr::tbl(con, '__types') %>%
        dplyr::collect() %>%
        dplyr::filter(table != name)

      # Combine metadata
      new_meta <- dplyr::bind_rows(meta, other_meta) %>%
        dplyr::arrange(table, var)

      # Write to metadata to database
      DBI::dbWriteTable(con, '__types', new_meta, overwrite = TRUE)
    }

    # Write data to database
    if (!DBI::dbExistsTable(con, name)) {
      DBI::dbCreateTable(con, name, data)
      invisible(DBI::dbAppendTable(con, name, data))
    } else {
      DBI::dbWriteTable(con, name, data, overwrite = overwrite, ...)
    }

  })

}

# ===========================
#' Load
#' Load a named dataframe from a database, where it is stored in decomposed form:
#'   - a named table with a dumbed-down version of the dataframe, where logicals are
#'     coerced to integers, and factors are coerced to strings.
#'   - a metadata table called `__types`, with the information necessary to
#'     recover the logicals and factors in the original dataframe.
#' The dataframe can be saved back to the database using [saveDF()].
#'
#' @param con  An SQL database connection (generated via [DBI::dbConnect()]).
#' @param name The name of the table to load from the database.
#'
#' @seealso Pure functions to decompose/recompose dataframes: [decomposeDF()] [recomposeDF()]
#' @export
#' @examples
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#'
#' df <- tibble::tibble(
#'   a = factor(c("a", "a", "b"), levels = c("a", "b"), ordered = TRUE),
#'   b = c(TRUE, TRUE, FALSE),
#'   c = c(1,2,3)
#'  )
#'
#' saveDF(con, df, "test_1")
#'
#' loadDF(con, "test_1")
#'
#' DBI::dbDisconnect(con)
#' @importFrom magrittr "%>%"
loadDF <- function(con, name) {
  # Show stoppers
  if (!DBI::dbIsValid(con))
    stop('Database connection is closed.')
  if (!DBI::dbExistsTable(con, '__types'))
    stop('Metadata table __rtypes does not exist in the database.')
  if (!DBI::dbExistsTable(con, name))
    stop('Table does not exist in the database.')

  # Assemble the decomposed representation
  decomposed <- DBI::dbWithTransaction(con, list(
    # Load metadata from database
    meta = dplyr::tbl(con, '__types') %>%
      dplyr::filter(table == name) %>%
      dplyr::select(-table) %>%
      dplyr::collect(),

    # Load dumb data from database
    data = dplyr::tbl(con, name) %>%
      dplyr::collect()
  ))

  # Recompose the dataframe, recovering logicals and factors
  recomposeDF(decomposed)
}
