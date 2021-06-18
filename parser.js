const csv = require('csv-parser');
const axios = require('axios');
const fs = require('fs');

const LOGTAG = "~~ [TL TACTIC TOOL]: ";

// CHANGE THIS (TODO: read from args)
const tacticFileName = "tactic.csv";
// const tacticFileName = "tactic-small.csv";


var expectedTotalRequests = 0;
var totalRequestCount = 0;
var interval;

var successCount = 0;
var failureCount = 0;
var failedURLMap = [];             // After processing the urls to see which ones are successful


var tacticData = [];                 // processCSV will provide this if successful.
var jsImpURLCollection = {};              // After the process CSV step, we'll provide this to our batching function

var logger = (msg) => {
  console.log(LOGTAG + msg);
}

// Read in the CSV file and process it.
var processCSV = () => {

  // Read in the tactic data
  fs.createReadStream(tacticFileName)
    .pipe(csv())
    .on('data', (row) => {
      tacticData.push(row);
    })
    .on('end', () => {
      logger('CSV file successfully processed');
      // console.log(tacticData);
      jsImpURLCollection = obtainImpressionURLs(tacticData);
      // console.log(jsImpURLCollection);
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
      //console.log("url targets:", urlTargets.length);

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
  // console.log(urlTargets)
  return true;
}


// Grab the urls from each tactic
var cleanURLTargets = (urlArr) => {
  // console.log(urlArr);
  var collection = [];
  var splitByQuotes = urlArr.split("\"");
  splitByQuotes.map(function (a) {
    if (a.length > 1) {
      collection.push(a);
    }
  });
  // console.log("Finished: ", collection);
  return collection;
}


// To be used with timeout to periodically query the status of the job
var checkProcessingStatus = () => {
  // Report whenever we can.
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(`Current Status: [${totalRequestCount}/${expectedTotalRequests}] ${Math.round(100*totalRequestCount/expectedTotalRequests)}%`);
}


// Process the data with the provided delay and batch size 
var processBatchAfterDelayMS = (collection, delay, startingIndex, batchSize) => {
  setTimeout(function () {
    for (var i = startingIndex; i < startingIndex + batchSize; ++i) {
      var t = collection[i];
      if (t) {
        totalRequestCount++;
        doRequestForURLAndTactic(totalRequestCount, t.impression_url_arr[0], t.tactic_id);
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



var doRequestForURLAndTactic = (reqID, url, tactic) => {
  // logger(`[${reqID}/${expectedTotalRequests}] Tactic: ${tactic} URL: ${url}`);
  try {
    url = JSON.parse(`"${url}"`);     // Hacky - there's got to be a better way
  } catch (e) {

    // If a url fails the JSON parse for whatever reason, log it as a failure and short terminate. (for now)

    failureCount++;
    failedURLMap.push([tactic, url]);
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
    failedURLMap.push([tactic, url]);
  }).finally(function () {

    // Print the results when we are done.
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
  processBatchAfterDelayMS(collection, 40, 0, 20);
  // Set up a logger to periodically report status back
  interval = setInterval(checkProcessingStatus, 1000); 

}



var printResults = () => {

  // Success: 3246, Failure: 20618
  // Success: 3246, Failure: 20618


  //   Success: 18710, Failure: 5154
  //   Success: 18997, Failure: 4867
  //  Success: 18965, Failure: 4899


  logger(`Success: ${successCount}, Failure: ${failureCount}`);
  logger("Failed URLs and their tactic: ", failedURLMap);

}





// Invoke the process.
processCSV();
