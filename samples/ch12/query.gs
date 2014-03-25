/**
 * BigQuery query script.
 * This script installs a 'BigQuery' menu in your spreadsheet that
 * allow you to run queries.
 *
 * To use, first enter your project ID in a
 * cell of your choosing, then with that cell selected, select the
 * 'Set project' item from the BigQuery menu. This will save your
 * project ID as a user property so you won't need to set it again.
 * You can delete the cell where you had entered the project ID.
 * To run a query, write a query in any cell or range of cells.
 * Select those cells, and then pick 'Run Query' from the BigQuery
 * menu. This will run the query you've selected and add the outputs
 * to a new sheet called 'Query Results'. If this sheet already
 * existed, it will be cleared and rewritten (so don't put stuff you
 * want to keep in the results sheet). Note that Google Spreadsheets
 * has a limit of about 400,000 cells, so if you have large query
 * results, you might find yourself hitting this limit pretty quickly.
 */

// Name of the sheet that will contain the query results. This page
// will be cleared every time the query is run.
var QUERY_RESULTS_SHEET = 'Query Results'
// Name of the user property where we save the project ID.
var PROJECT_ID_PROPERTY = 'PROJECT_ID'
// Number of results to return from BigQuery per page.
var MAX_RESULTS_PER_PAGE = 1000

/** Installs a BigQuery menu to allow you to run queries. */
function onOpen() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  var menuEntries = [];
  // Create a menu link to the 'runQuery' function. To run a query,
  // select the cells that contain your query and then select 'Run
  // a query  from the BigQuery menu.
  menuEntries.push({name: "Run a query", functionName: "runQuery"});

  // Before you can run a query, you need to set a project ID to use
  // with the query. You can do this by selecting a cell that has the
  // project ID to use, then picking the the 'Set project" menu item
  // from the BigQuery menu. Note that after you set the project ID,
  // you can delete that cell... the project ID is remembered as part
  // of the user properties for the script.
  menuEntries.push({name: "Set project", functionName: "setProject"});
  ss.addMenu("BigQuery", menuEntries);
}

/**
 * Sets the project ID to use for BigQuery queries. User
 * should select a cell containing the desired project ID
 * and then call the setProject menu item.
 */
function setProject() {
  var sheet = SpreadsheetApp.getActiveSheet();
  var cell = sheet.getActiveCell();
  var value = cell.getValue();
  if (value) {
    projectId = String(value);
    UserProperties.setProperty(PROJECT_ID_PROPERTY, projectId);
    Logger.log('Project ID set to %s', projectId);
  } else {
    Logger.log('Project ID not set');
  }
}

/**
 * Gets the project ID property.
 */
function getProjectId() {
  var projectId = UserProperties.getProperty(PROJECT_ID_PROPERTY);
  if (!projectId) {
    throw new Error("Property PROJECT_ID is not registered");
  }
  return projectId;
}

/**
 * Reads the query from the active selection in the current
 * spreadsheet. To read from more than one cell, just select the range
 * of cells you want.
 */
function readQuery() {
  var sheet = SpreadsheetApp.getActiveSheet();
  var range = sheet.getActiveRange();
  var values = range.getValues();
  query = values.join(' ');
  return query;
}

/**
 * Given a sheet to write to, the start row and a 2D array of
 * values, writes those values to the sheet.
 */
function writeChunk(sheet, startIndex, data) {
  var nRows = data.length;
  if (nRows == 0) {
    // Nothing to do.
    Logger.log('No results to write');
    return;
  }
  var nCols = data[0].length;
  sheet.getRange(startIndex, 1, nRows, nCols).setValues(data);
  Logger.log('Wrote %s rows to: %s', nRows, sheet.getName());
}

/**
 * Translates the .f and .v format of the query results into
 * a 2D array of values.
 */
function extractRows(rows) {
  // Append the results.
  var data = new Array(rows.length);
  for (var i = 0; i < rows.length; i++) {
    var cols = rows[i].f;
    data[i] = new Array(cols.length);
    for (var j = 0; j < cols.length; j++) {
      data[i][j] = cols[j].v;
    }
  }
  return data;
}

/**
 * Sets up a sheet with a given name which will contain our
 * query results. If the sheet already exists, it will be cleared.
 * If it doesn't exist, we'll create a new one.
 */
function setUpResultSheet(sheetName) {
  var spreadsheet = SpreadsheetApp.getActive();
  var sheet = spreadsheet.getSheetByName(sheetName);

  if (spreadsheet.getActiveSheet().getName() == sheetName) {
    // Don't run a query from the queryResults sheet -- this will
    // erase your query!
    throw new Error("Cannot write query results to active sheet");
  }

  if (sheet) {
    sheet.clear();
  } else {
    sheet = spreadsheet.insertSheet(sheetName);
  }
  return sheet;
}

/**
 * Gets a function that can be used to write out chunks of
 * data as they're being returned from BigQuery.
 */
function getChunkWriter(sheetName) {
  return function(rowIndex, rows) {
    var sheet;
    if (rowIndex == 1) {
      // If we're starting from the beginning, we need to set up
      // the result  sheet.
      sheet = setUpResultSheet(sheetName);
    } else {
      // The sheet should already exist and be ready for us to write.
      sheet = SpreadsheetApp.getActive().getSheetByName(sheetName);
    }
    writeChunk(sheet, rowIndex, rows);
    return rowIndex + rows.length;
  }
}


/**
 * Pulls the field names out of query results. Retrurns them
 * as a 2D array with 1 row, so they can be written via the
 * same mechanism we use to write rows of data.
 */
function extractHeaders(fields) {
  return [fields.map(function(field) {return field.name;})];
}

/**
 * Runs a given SQL query and writes the results to the chunkWriter
 * one page at a time. This is preferable to returning results, since
 * the query results may be large.
 */
function runSqlQuery(projectId, sql, chunkWriter) {
  Logger.log('%s: Running query: %s', projectId, sql);
  var resource = {
    'query': sql,
    'maxResults': MAX_RESULTS_PER_PAGE
  };
  var queryResults = BigQuery.Jobs.query(resource, projectId);

  var jobId = queryResults.jobReference.jobId;

  // The job might not actually be done; wait until it is marked
  // complete. For simple queries, it will have completed within the
  // default timeout, but for more complex queries (JOIN, etc), you
  // might find the query takes a long time.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Write the field names as the first row.
  chunkWriter(1, extractHeaders(queryResults.schema.fields));
  // Now we've got the first page, write that out to the spreadsheet.
  nextIndex = chunkWriter(2, extractRows(queryResults.rows));

  // But wait, there's more!
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      'pageToken': queryResults.pageToken,
      'maxResults': MAX_RESULTS_PER_PAGE
    });

    nextIndex = chunkWriter(nextIndex, extractRows(queryResults.rows));
  }
}

/**
 * Runs a query from the currently selected cells and write the results
 * to a sheet called 'Query Results' in the current spreadsheet.
 */
function runQuery() {
   var chunkWriter = getChunkWriter(QUERY_RESULTS_SHEET);
   runSqlQuery(getProjectId(), readQuery(), chunkWriter);
}

