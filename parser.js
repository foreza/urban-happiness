const csv = require('csv-parser');
const axios = require('axios');
const fs = require('fs');


/* Global Vars */

const LOGTAG = "~~ [TL TOOL]: ";      // Logging constant
const BATCH_SIZE = 20;
const BATCH_DELAY_MS = 50;

// For reporting requests
var interval;                         // Reference to the logger setInterval object so we can destroy later
var expectedTotalRequests = 0;        // Tracks the # of total requests so we don't have to continue asking for the object length
var totalRequestCount = 0;            // Incremented every time a single request is queued up - consumed by logger

var successCount = 0;                 // Tracks the # of successful http requests (2xx/3xx)
var failureCount = 0;                 // Tracks the # of failed requests (4xx,5xx,timeout, invalid url)
var failedURLMap = [];                // Stores failed urls to be output later at the end of the batch run

// Utility for logging messages
var logger = (msg) => {
  console.log(LOGTAG + msg);
}


// Read in the CSV file as the first argument and process it.
var processCSV = () => {

  var myArgs = process.argv.slice(2);
  logger("Processing: " + myArgs[0]);

  var tacticData = [];
  var jsImpURLCollection = {};

  // Read in the tactic data
  fs.createReadStream(myArgs[0])
    .pipe(csv())
    .on('data', (row) => {
      tacticData.push(row);
    })
    .on('end', () => {
      logger('CSV file successfully read, checking impression URLs');
      jsImpURLCollection = obtainImpressionURLs(tacticData);
      verifyImpressionURLs(jsImpURLCollection);
    });
}


// Grab the impression URLs and associated tactic from the CSV raw data
var obtainImpressionURLs = (data) => {

  var impURLCollection = [];

  // Format it the way we need.
  for (var i = 0; i < data.length; ++i) {

    var urlTargets = data[i].impression_pixel_json.split(",");    // There may be 0, 1, or more URLs
    var currTactic = data[i].tactic_id;

    // Verify that the url(s) are valid
    if (isValidURLCollection(urlTargets)) {

      // Loop through each valid row
      for (var j = 0; j < urlTargets.length; ++j) {

        // Clean up the url targets for the row
        var cleanedURLArr = cleanURLTargets(urlTargets[j]);
        impURLCollection.push({
          tactic_id: currTactic,
          impression_url_arr: cleanedURLArr
        })
      }

    } else {
      // TODO: Handle this!
    }
  }

  return impURLCollection;
}


// Verify the tactic has provided valid impression URLs
var isValidURLCollection = (urlTargets) => {
  if (urlTargets.length == 0 || urlTargets[0] == "[]" || urlTargets == "NULL") {
    return false;
  }
  return true;
}


// Grab the urls from each tactic
var cleanURLTargets = (urlArr) => {
  var collection = [];
  var splitByQuotes = urlArr.split("\"");
  splitByQuotes.map(function (a) {
    if (a.length > 1) {
      collection.push(a);
    }
  });
  return collection;
}


// To be used with timeout to periodically query the status of the job
var checkProcessingStatus = () => {
  // Report whenever we can.
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(`Current Status: [${totalRequestCount}/${expectedTotalRequests}] ${Math.round(100 * totalRequestCount / expectedTotalRequests)}%`);
}


// Process the data with the provided delay and batch size 
var processBatchAfterDelayMS = (collection, delay, startingIndex, batchSize) => {
  setTimeout(function () {
    for (var i = startingIndex; i < startingIndex + batchSize; ++i) {
      var t = collection[i];
      if (t) {
        totalRequestCount++;
        doRequestForURLAndTactic(t.impression_url_arr[0], t.tactic_id);
      } else {
        // TODO: handle this
        // We shouldn't reach here...  
      }

    }

    if (startingIndex + batchSize >= collection.length) {
      logger("Finished processing!");
      clearInterval(interval);    // Kill the update interval
      return;
    } else {
      processBatchAfterDelayMS(collection, delay, startingIndex + batchSize, batchSize);    // Call the next batch.
    }

  }, delay);

}


// Uses axios to do a request and report on the status
var doRequestForURLAndTactic = (url, tactic) => {
  try {
    url = JSON.parse(`"${url}"`);     // TODO: Hacky - there's got to be a better way
  } catch (e) {
    // If a url fails the JSON parse for whatever reason, log it as a failure and short terminate. (for now)
    failureCount++;
    failedURLMap.push({ tactic: tactic, url: url });
    return;
  }

  axios.get(url, { timeout: 1000 }).then(function (response) {

    // Check to ensure our response is a 2XX or 3XX
    if (response.status.toString()[0] === "2" || response.status.toString()[0] === "3") {
      successCount++;
    }
  }).catch(function (error) {

    // If the request failed for whatever reason, add to the failure count. Timeouts, 404, etc
    failureCount++;
    failedURLMap.push({ tactic: tactic, url: url });
  }).finally(function () {

    // Only actually report the results when we are done - we shouldn't drop anything.
    if (failureCount + successCount == expectedTotalRequests) {
      printResults();
    }

  });


}


// Starts the batch job.
var verifyImpressionURLs = (collection) => {
  /*
    Batch these in groups of "X" so we're not overloading our poor server / computer...
    Queue up batchSize # of requests and execute them.
    TODO: Tweak these to find a good balance.
  */
  expectedTotalRequests = collection.length;
  processBatchAfterDelayMS(collection, BATCH_DELAY_MS, 0, BATCH_SIZE);
  // Set up a logger to periodically report status back
  interval = setInterval(checkProcessingStatus, 1000);
}


// Print out the results.
var printResults = () => {
  logger("CSV BATCH RUN SUMMARY:");
  console.log("========================")
  console.log(`\n\nNumber OK (2xx and 3xx Responses):\n${successCount}/${totalRequestCount}
  \nNumber Failed (4xx and 5xx Responses):\n${failureCount}/${totalRequestCount}\n\n`);
  // List the Tactic ID & URLs that failed
  prettyPrintFailureURL(failedURLMap);
}


// Pretty print out the failure URLs so we can consume this elsewhere if we desire
var prettyPrintFailureURL = (urlArr) => {
  // console.log("Failed URLs and their tactic in CSV: \n ");
  // console.log("========================")
  // console.log("Tactic,URL")
  // for (var i = 0; i < urlArr.length; ++i) {
  //   console.log(`${urlArr[0]}, ${urlArr[1]}`)
  // }
  // console.log("========================")


  let filename = `failed-url-results-${Date.now()}.csv`;
  const createCsvWriter = require('csv-writer').createObjectCsvWriter;
  const csvWriter = createCsvWriter({
    path: filename,
    header: [
      { id: 'tactic', title: 'Tactic' },
      { id: 'url', title: 'URL' },
    ]
  });

  csvWriter.writeRecords(urlArr)
    .then(() => logger(`${filename} has been created. \n There are ${urlArr.length} entries to review.`));


}


// Actual entry point.
processCSV();
