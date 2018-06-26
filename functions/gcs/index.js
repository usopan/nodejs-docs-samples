/**
 * Copyright 2016, Google, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

// [START functions_word_count_setup]
const Storage = require('@google-cloud/storage');
const readline = require('readline');
const parse = require('csv-parse');
// Instantiates a client
const storage = Storage();
// [END functions_word_count_setup]
const BigQuery = require('@google-cloud/bigquery');
const projectId = 'secure-unison-207809';
const datasetId = 'healthdata';
const _ = require('underscore');

function getFileStream(file) {
  if (!file.bucket) {
    throw new Error('Bucket not provided. Make sure you have a "bucket" property in your request');
  }
  if (!file.name) {
    throw new Error('Filename not provided. Make sure you have a "name" property in your request');
  }

  return storage.bucket(file.bucket).file(file.name).createReadStream();
}

function createTable(tableId) {
  // [START bigquery_create_table]
  // Imports the Google Cloud client library
  return new Promise(function (resolve, reject) {
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    bigquery
      .dataset(datasetId)
      .table(tableId)
      .exists().then(exists => {
        const result = exists[0];
        if (!result) {
          let schema = {
            Member_ID: 'integer',
            First_Name: 'string',
            Last_Name: 'string',
            Gender: 'string',
            Age: 'integer',
            Height: 'float',
            Weight: 'integer',
            Hours_Sleep: 'integer',
            Calories_Consumed: 'integer',
            Exercise_Calories_Burned: 'integer',
            Date: 'Date',
            recommended_Min_Sleep: 'integer',
            recommended_Max_Sleep: 'integer',
            recommendedSedentaryCalories: 'integer',
            recommendedModerateCalories: 'integer',
            recommendedActiveCalories: 'integer'
          }
          // For all options, see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
          const options = {
            schema: schema,
          };
          bigquery
            .dataset(datasetId)
            .createTable(tableId, options)
            .then(results => {
              const table = results[0];
              console.log(`Table ${table.id} created.`);
              resolve();
            })
            .catch(err => {
              console.error('ERROR:', err);
              reject();
            });
        } else {
          resolve();
        }
      })
  })
}
function insertRowsAsStream(tableId, rows, cb) {

  return new Promise(function (resolve, reject) {
    createTable(tableId).then(function () {
      const bigquery = new BigQuery({
        projectId: projectId,
      });

      // Inserts data into a table
      bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(rows)
        .then(() => {
          console.log(`Inserted ${rows.length} rows`);
          resolve();

        })
        .catch(err => {
          if (err && err.name === 'PartialFailureError') {
            if (err.errors && err.errors.length > 0) {
              console.log('Insert errors:');
              err.errors.forEach(err => console.error(err));
            }
          } else {
            console.error('ERROR:', err);
            reject(err, "error");
          }
        });
    }).catch(function (error) {
      reject(error, "error");
    })
  });
  // [END bigquery_table_insert_rows]
}

function getCDCSleepRequirements() {
  return new Promise(function (resolve, reject) {
    let sleep = storage.bucket('healthapp').file('cdc_sleep_hours_lookup.csv').createReadStream();
    // let options = {
    //   input: sleep
    // };
    let sleepRecommendations = [];
    let parser = parse({ columns: true, cast: true }, function (err, data) {
      // console.log(data);
      resolve(data);
    });
    sleep.pipe(parser);

  });
}

function getCDCCalorieRequirements() {
  return new Promise(function (resolve, reject) {
    let calrories = storage.bucket('healthapp').file('cdc_calorie_needs_lookup.csv').createReadStream();
    // let options = {
    //   input: sleep
    // };
    let calorieRecommendations = [];
    let parser = parse({ columns: true, cast: true }, function (err, data) {
      // console.log(data);
      resolve(data);
    });
    calrories.pipe(parser);
  });
}

function mapValueWithRecommendation(data, cdcRecoArray, cb, file) {
  let sleepRecommendations = cdcRecoArray[1];
  let calorieRecommendations = cdcRecoArray[0];
  let sleepReco = null;
  let result = data.map(member => {
    // console.log(member, sleepRecommendations);
    sleepRecommendations.some(sleep => {
      if (member.Age >= sleep.Min_Age && member.Age <= sleep.Max_Age) {
        sleepReco = sleep;
        return true;
      } else {
        return false;
      }
    });
    let calorieReco = null;
    calorieRecommendations.some(calorie => {
      if (member.Age >= calorie.Min_Age && member.Age <= calorie.Max_Age && member.Gender === calorie.Gender) {
        calorieReco = calorie;
        return true;
      } else {
        return false;
      }
    });
    let finalRecord = Object.assign({}, member, { recommended_Min_Sleep: sleepReco.Min_Sleep_Hours_Per_Day, recommended_Max_Sleep: sleepReco.Max_Sleep_Hours_Per_Day, recommendedSedentaryCalories: calorieReco.Sedentary, recommendedModerateCalories: calorieReco.Moderately_Active, recommendedActiveCalories: calorieReco.Active })
    // console.log(finalRecord);
    return finalRecord;
  });
  let groupedRowsPromises = [];
  _.chain(result).groupBy('Member_ID').map(function (value, key) {
    let tableId = 'Member_' + key;
    let rowPromise = insertRowsAsStream(tableId, value, cb);
    groupedRowsPromises.push(rowPromise);
  });
  Promise.all(groupedRowsPromises).then(function () {
    storage.bucket(file.bucket).file(file.name).delete();
    cb(null, "DOne");
  });


  return result;
}
// [START functions_word_count_read]
/**
 * Reads file and responds with the number of words in the file.
 *
 * @example
 * gcloud alpha functions call cdcRecommendation --data '{"bucket":"YOUR_BUCKET_NAME","name":"sample.txt"}'
 *
 * @param {object} event The Cloud Functions event.
 * @param {object} event.data A Google Cloud Storage File object.
 * @param {string} event.data.bucket Name of a Cloud Storage bucket.
 * @param {string} event.data.name Name of a file in the Cloud Storage bucket.
 * @param {function} callback The callback function.
 */
exports.cdcRecommendation = (event, cb) => {
  const file = event.data;

  if (file.resourceState === 'not_exists') {
    // This is a file deletion event, so skip it
    callback();
    return;
  }
  let testFile = {
    bucket: 'healthapp',
    name: 'member_fitness_tracker_history.csv'
  }
  let uploadFile = getFileStream(file);
  let lookupPromise = Promise.all([getCDCCalorieRequirements(), getCDCSleepRequirements()]);
  console.log(event);
  lookupPromise.then(function (resultArray) {
    let parser = parse({ columns: true, cast: true }, function (err, data) {
      // console.log(data);
      let result = mapValueWithRecommendation(data, resultArray, cb, file);


    });
    uploadFile.pipe(parser);

  }).catch(function (error) {
    cb(error, 'error');
    // throw new Error(error);
  });
};
// [END functions_word_count_read]
