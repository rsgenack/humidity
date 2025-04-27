// Combined script to fetch all historical SensorPush data and load into BigQuery

import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
import fs from 'fs';
import path from 'path';

// --- Configuration ---

// ** Credentials (Set via Environment Variables) **
const SENSORPUSH_EMAIL = process.env.SENSORPUSH_EMAIL;
const SENSORPUSH_PASSWORD = process.env.SENSORPUSH_PASSWORD;
// GOOGLE_APPLICATION_CREDENTIALS environment variable should be set to the path of your service account key file.

// Check for essential credentials
if (!SENSORPUSH_EMAIL || !SENSORPUSH_PASSWORD) {
    console.error("ERROR: Missing SENSORPUSH_EMAIL or SENSORPUSH_PASSWORD environment variables.");
    process.exit(1);
}
if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
     console.warn("WARNING: GOOGLE_APPLICATION_CREDENTIALS environment variable not set. Authentication might fail unless running in a GCP environment with implicit credentials.");
}

// ** Project/Dataset/Table Configuration (Defaults can be overridden by Env Vars) **
const GOOGLE_PROJECT_ID = process.env.GOOGLE_PROJECT_ID || 'savvy-fountain-431023-t4';
const BIGQUERY_DATASET_ID = process.env.BIGQUERY_DATASET_ID || 'Humidity';
const BIGQUERY_TABLE_ID = process.env.BIGQUERY_TABLE_ID || 'all_data';

// ** API/Batch Limits (Defaults can be overridden by Env Vars) **
const BIGQUERY_BATCH_SIZE = parseInt(process.env.BIGQUERY_BATCH_SIZE || '500', 10);
const SENSORPUSH_API_LIMIT = parseInt(process.env.SENSORPUSH_API_LIMIT || '5000', 10);

// ** Sensor Mapping (Read from sensors.json) **
let SENSOR_MAPPING = {};
const sensorsFilePath = path.join(__dirname, 'sensors.json');
try {
    if (fs.existsSync(sensorsFilePath)) {
        const sensorsFileContent = fs.readFileSync(sensorsFilePath, 'utf8');
        SENSOR_MAPPING = JSON.parse(sensorsFileContent);
        log(`Loaded sensor mapping from ${sensorsFilePath}`);
        if (Object.keys(SENSOR_MAPPING).length === 0) {
             throw new Error('sensors.json is empty or contains no sensor mappings.');
        }
        // Validate the structure briefly
        Object.values(SENSOR_MAPPING).forEach(sensorInfo => {
            if (!sensorInfo.name || !sensorInfo.id) {
                throw new Error(`Invalid sensor entry in sensors.json: missing 'name' or 'id'. Entry: ${JSON.stringify(sensorInfo)}`);
            }
        });
    } else {
        throw new Error(`Configuration file not found: ${sensorsFilePath}. Please create it based on sensors.example.json.`);
    }
} catch (error) {
    logError(`Failed to load or parse ${sensorsFilePath}`, error);
    process.exit(1);
}

// --- Helper Functions ---

/**
 * Logs a message with a timestamp.
 * @param {string} message The message to log.
 */
function log(message) {
    console.log(`[${new Date().toISOString()}] ${message}`);
}

/**
 * Logs an error message with details.
 * @param {string} message The error message.
 * @param {Error} [error] Optional error object.
 */
function logError(message, error) {
    console.error(`[${new Date().toISOString()}] ERROR: ${message}`);
    if (error) {
        // Log specific parts of Axios errors if available
        if (error.response) {
            console.error('  Response Status:', error.response.status);
            console.error('  Response Data:', JSON.stringify(error.response.data, null, 2));
        } else if (error.request) {
            console.error('  No response received for request:', error.request);
        } else {
            console.error('  Error Details:', error.message);
        }
        if (error.stack) {
            console.error('  Stack Trace:', error.stack);
        }
    }
}

// --- Main Logic ---

/**
 * Fetches the SensorPush authorization token.
 */
async function getAuthorizationToken() {
    log("Step 1: Requesting SensorPush authorization token...");
    try {
        const response = await axios({
            method: "post",
            url: "https://api.sensorpush.com/api/v1/oauth/authorize",
            headers: { "Content-Type": "application/json" },
            data: {
                "email": SENSORPUSH_EMAIL,         // Use environment variable
                "password": SENSORPUSH_PASSWORD      // Use environment variable
            }
        });
        const authToken = response.data.authorization;
        if (!authToken) {
            throw new Error("Authorization token not found in SensorPush response.");
        }
        log("Authorization token obtained successfully.");
        return authToken;
    } catch (error) {
        logError("Failed to obtain SensorPush authorization token.", error);
        throw error; // Re-throw to stop execution
    }
}

/**
 * Fetches the SensorPush access token using the authorization token.
 * @param {string} authorizationToken The authorization token.
 */
async function getAccessToken(authorizationToken) {
    log("Step 2: Requesting SensorPush access token...");
    try {
        const response = await axios({
            method: "post",
            url: "https://api.sensorpush.com/api/v1/oauth/accesstoken",
            headers: { "Content-Type": "application/json" },
            data: { "authorization": authorizationToken }
        });
        const accessToken = response.data.accesstoken;
         if (!accessToken) {
            throw new Error("Access token not found in SensorPush response.");
        }
        log("Access token obtained successfully.");
        return accessToken;
    } catch (error) {
        logError(`Failed to obtain SensorPush access token. Auth token start: ${String(authorizationToken).substring(0,5)}...`, error);
        throw error; // Re-throw to stop execution
    }
}

/**
 * Fetches the list of sensor devices registered to the account.
 * @param {string} accessToken The SensorPush access token.
 * @returns {Promise<object>} A map of device IDs to device names.
 */
async function fetchSensorDevices(accessToken) {
    log("Step 2a: Fetching list of registered SensorPush devices...");
    try {
        const response = await axios({
            method: "post", // Use POST as per SensorPush examples for consistency
            url: "https://api.sensorpush.com/api/v1/devices",
            headers: {
                "Authorization": accessToken,
                "Content-Type": "application/json"
            },
            data: {} // Empty body
        });

        const devices = response.data; // Should be an object { device_id: { name: ... }, ... }
        if (!devices || typeof devices !== 'object') {
            throw new Error("Unexpected format received from /devices endpoint.");
        }

        const deviceCount = Object.keys(devices).length;
        log(` -> Found ${deviceCount} registered devices in the account.`);

        if (deviceCount > 0) {
            log(" -> Registered Device List (Name: ID):");
            for (const [id, info] of Object.entries(devices)) {
                console.log(`      - ${info.name}: ${id}`);
            }
            log(" -> Use the IDs above to help configure your sensors.json file.");
        }

        return devices;
    } catch (error) {
        logError("Failed to fetch SensorPush device list. Will proceed without this information.", error);
        return null; // Return null on error, allow the script to continue
    }
}

/**
 * Fetches all historical sensor data from SensorPush API with pagination.
 * @param {string} accessToken The SensorPush access token.
 */
async function fetchAllSensorData(accessToken) {
    log(`Step 3: Fetching ALL historical data for ${Object.keys(SENSOR_MAPPING).length} sensors defined in sensors.json...`);
    let allSensorReadings = []; // Flat array to store all readings
    const fetchErrors = [];

    // Iterate through the sensor configuration object
    for (const [configKey, sensorInfo] of Object.entries(SENSOR_MAPPING)) {
        const { name: sensorName, id: sensorId, room } = sensorInfo; // Destructure using the new structure
        log(` -> Processing sensor: ${sensorName} (ID: ${sensorId}, Config Key: ${configKey})`);

        const startTimestamp = '1970-01-01T00:00:00Z'; // Start from epoch
        let moreDataAvailable = true;
        let currentStartTime = startTimestamp;
        let sensorSpecificReadings = [];
        let batchNum = 1;

        while (moreDataAvailable) {
            log(`    Fetching batch ${batchNum} for ${sensorName} starting from ${currentStartTime}`);
            try {
                const response = await axios({
                    method: "post",
                    url: "https://api.sensorpush.com/api/v1/samples",
                    headers: {
                        "Authorization": accessToken,
                        "Content-Type": "application/json"
                    },
                    data: {
                        "sensors": [sensorId],
                        "limit": SENSORPUSH_API_LIMIT,
                        "startTime": currentStartTime
                    }
                });

                const fetchedData = response.data;
                const samplesReturned = fetchedData?.samples_returned ?? 0;
                log(`      Received ${samplesReturned} samples.`);

                if (fetchedData.sensors && fetchedData.sensors[sensorId] && fetchedData.sensors[sensorId].length > 0) {
                    // Process and add the custom sensor_name and room
                    const newReadings = fetchedData.sensors[sensorId].map(entry => ({
                        ...entry,
                        sensor_name: sensorName, // Use the custom name from sensorInfo.name
                        room: room || 'Unknown' // Use room from sensorInfo, default if missing
                    }));
                    sensorSpecificReadings.push(...newReadings);

                    if (samplesReturned === SENSORPUSH_API_LIMIT) {
                        const lastTimestamp = newReadings[newReadings.length - 1].observed;
                        const nextStartTime = new Date(new Date(lastTimestamp).getTime() + 1);
                        currentStartTime = nextStartTime.toISOString();
                        moreDataAvailable = true;
                        batchNum++;
                    } else {
                        moreDataAvailable = false;
                        log(`      Finished fetching all data for ${sensorName}.`);
                    }
                } else {
                    moreDataAvailable = false;
                    log(`      No further data found for ${sensorName} starting from ${currentStartTime}.`);
                }

            } catch (apiError) {
                logError(`API Error during batch fetch for ${sensorName} (start: ${currentStartTime})`, apiError);
                fetchErrors.push(`API Error for sensor ${sensorName} (start: ${currentStartTime}): ${apiError.message}`);
                moreDataAvailable = false; // Stop fetching for this sensor on error
            }
        } // End while loop (pagination)

        log(` -> Collected total ${sensorSpecificReadings.length} historical entries for ${sensorName}.`);
        allSensorReadings.push(...sensorSpecificReadings); // Add to the main flat array

    } // End for loop (sensors)

    log(`Step 3 Complete: Total historical readings fetched across all sensors: ${allSensorReadings.length}`);
    if (fetchErrors.length > 0) {
        logError(`Encountered ${fetchErrors.length} errors during data fetch:`);
        fetchErrors.forEach((err, i) => console.error(`   ${i + 1}: ${err}`));
    }
    return allSensorReadings; // Return the flat array
}

/**
 * Processes the raw sensor data into the format for BigQuery.
 * @param {Array<object>} rawReadings Flat array of readings from fetchAllSensorData.
 */
function processSensorData(rawReadings) {
    log(`Step 4: Processing ${rawReadings.length} raw readings...`);
    const processedData = [];
    let skippedCount = 0;

    rawReadings.forEach(reading => {
        // Basic validation - sensor_name and room are now added during fetch
        if (!reading || typeof reading.observed === 'undefined' || typeof reading.humidity === 'undefined' || !reading.sensor_name) {
            // console.warn(`Skipping invalid reading:`, reading); // Uncomment for debugging
            skippedCount++;
            return;
        }

        processedData.push({
            observed: reading.observed, // Keep ISO format string
            temperature: reading.temperature,
            humidity_percent: reading.humidity / 100, // Convert to fraction
            dewpoint: reading.dewpoint,
            vpd: reading.vpd,
            sensor_name: reading.sensor_name, // Already contains the custom name
            room: reading.room, // Already contains the room name
        });
    });

    log(`Step 4 Complete: Processed ${processedData.length} valid readings. Skipped ${skippedCount} invalid readings.`);
    if (processedData.length > 0) {
        log(`Sample processed reading: ${JSON.stringify(processedData[0], null, 2)}`);
    }
    return processedData;
}

/**
 * Loads data into BigQuery, handling deduplication and batch insertion.
 * @param {Array<object>} dataToLoad The processed data array.
 */
async function loadDataToBigQuery(dataToLoad) {
    log(`Step 5: Loading ${dataToLoad.length} processed records into BigQuery...`);
    const startTime = Date.now();
    let credentials;
    const loadErrors = [];
    let uniqueData = [];
    let duplicatesFound = 0;
    let totalInserted = 0;

    if (dataToLoad.length === 0) {
        log("No data to load into BigQuery. Skipping Step 5.");
        return { duplicatesFound: 0, rowsInserted: 0, errors: [] };
    }

    try {
        // --- Initialize BigQuery Client ---
        log(" -> Initializing BigQuery client...");
        try {
            credentials = JSON.parse(process.env.GOOGLE_CLOUD_CREDENTIALS);
        } catch (parseError) {
            throw new Error(`Invalid GOOGLE_CLOUD_CREDENTIALS format: ${parseError.message}`);
        }
        const bigquery = new BigQuery({ credentials, projectId: GOOGLE_PROJECT_ID });
        const dataset = bigquery.dataset(BIGQUERY_DATASET_ID);
        const table = dataset.table(BIGQUERY_TABLE_ID);
        const tablePath = `\`${GOOGLE_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}\``;
        log(" -> BigQuery client initialized.");

        // --- Deduplication ---
        log(` -> Starting deduplication against ${tablePath}...`);
        const uniqueSensorNames = [...new Set(dataToLoad.map(row => row.sensor_name))];
        const existingEntriesQuery = `
            SELECT DISTINCT sensor_name, observed
            FROM ${tablePath}
            WHERE sensor_name IN UNNEST(@sensor_names) AND observed IS NOT NULL
        `;
        const [existingRows] = await bigquery.query({
            query: existingEntriesQuery,
            params: { sensor_names: uniqueSensorNames },
        });

        const existingEntries = new Set(
            existingRows.map(row => {
                const observedTs = row.observed?.value || row.observed; // Handle BQ timestamp object/string
                return `${row.sensor_name}-${observedTs}`;
            })
        );
        log(` -> Found ${existingEntries.size} existing entries in BigQuery for relevant sensors.`);

        uniqueData = dataToLoad.filter(row => {
            const key = `${row.sensor_name}-${row.observed}`;
            return !existingEntries.has(key);
        });
        duplicatesFound = dataToLoad.length - uniqueData.length;
        log(` -> Deduplication complete: ${uniqueData.length} unique rows identified. Skipped ${duplicatesFound} duplicates.`);

        // --- Batch Insertion ---
        if (uniqueData.length > 0) {
            log(` -> Starting BigQuery insertion for ${uniqueData.length} unique rows...`);
            for (let i = 0; i < uniqueData.length; i += BIGQUERY_BATCH_SIZE) {
                const batch = uniqueData.slice(i, i + BIGQUERY_BATCH_SIZE);
                const batchNumber = i / BIGQUERY_BATCH_SIZE + 1;
                log(`    Inserting batch ${batchNumber} (${batch.length} rows)`);
                try {
                    const [response] = await table.insert(batch);
                    totalInserted += batch.length;

                    if (response && response.insertErrors && response.insertErrors.length > 0) {
                        const errorDetail = `BigQuery reported insertion errors for batch ${batchNumber}`;
                        logError(errorDetail, { message: JSON.stringify(response.insertErrors) });
                        loadErrors.push(`${errorDetail}: ${JSON.stringify(response.insertErrors)}`);
                        // Decide whether to continue or stop on partial failure
                    }
                } catch (insertError) {
                     const errorDetail = `BigQuery insertion API call failed for batch ${batchNumber}`;
                     logError(errorDetail, insertError);
                     loadErrors.push(`${errorDetail}: ${insertError.message}`);
                     // Decide whether to stop entirely on batch failure
                     // throw insertError; // Option to halt execution
                }
            }
            log(` -> Finished BigQuery insertions. Attempted insertion for ${totalInserted} rows.`);
        } else {
            log(" -> No unique data remaining after deduplication, nothing inserted.");
        }

    } catch (error) {
        logError("Critical error during BigQuery load step.", error);
        loadErrors.push(`Critical BQ Error: ${error.message}`);
        // Depending on where the error occurred, uniqueData might not be fully processed
    }

    const durationSec = ((Date.now() - startTime) / 1000).toFixed(2);
    log(`Step 5 Complete: BigQuery load finished in ${durationSec} seconds.`);
    return { duplicatesFound, rowsInserted: totalInserted, errors: loadErrors };
}

// --- Main Execution ---
async function main() {
    log("=== Starting SensorPush Full Historical Data Load ===");
    const overallStart = Date.now();
    let finalStatus = "SUCCESS";
    let summary = {};

    try {
        // Step 1 & 2: Authentication
        const authorizationToken = await getAuthorizationToken();
        const accessToken = await getAccessToken(authorizationToken);

        // Step 2a: Fetch and log device list (informational)
        const registeredDevices = await fetchSensorDevices(accessToken);

        // Optional: Compare registeredDevices with SENSOR_MAPPING from sensors.json
        if (registeredDevices) {
            const configuredSensorIds = new Set(Object.values(SENSOR_MAPPING).map(s => s.id));
            const registeredSensorIds = new Set(Object.keys(registeredDevices));

            // Check for sensors in account but not configured
            for (const registeredId of registeredSensorIds) {
                if (!configuredSensorIds.has(registeredId)) {
                    log(`WARNING: Sensor found in account but not in sensors.json -> ${registeredDevices[registeredId].name}: ${registeredId}`);
                }
            }

            // Check for sensors configured but not in account
            for (const configuredSensor of Object.values(SENSOR_MAPPING)) {
                if (!registeredSensorIds.has(configuredSensor.id)) {
                    log(`WARNING: Sensor ID in sensors.json not found in account -> ${configuredSensor.name}: ${configuredSensor.id}`);
                }
            }
        }

        // Step 3: Fetch All Data based on sensors.json mapping
        const rawReadings = await fetchAllSensorData(accessToken);

        // Step 4: Process Data
        const processedData = processSensorData(rawReadings);

        // Step 5: Load to BigQuery
        const loadResult = await loadDataToBigQuery(processedData);

        summary = {
            totalRawReadingsFetched: rawReadings.length,
            totalProcessedReadings: processedData.length,
            duplicatesSkipped: loadResult.duplicatesFound,
            rowsAttemptedForInsert: loadResult.rowsInserted,
            bigQueryLoadErrors: loadResult.errors.length,
        };
        if (loadResult.errors.length > 0) {
            finalStatus = "PARTIAL_FAILURE";
        }

    } catch (error) {
        logError("Pipeline execution failed with critical error.", error);
        finalStatus = "FAILURE";
        summary.criticalError = error.message;
    } finally {
        const overallDurationSec = ((Date.now() - overallStart) / 1000).toFixed(2);
        log("=== Pipeline Finished ===");
        log(`Status: ${finalStatus}`);
        log(`Total Duration: ${overallDurationSec} seconds`);
        log(`Summary: ${JSON.stringify(summary, null, 2)}`);
        if (finalStatus !== "SUCCESS") {
            process.exitCode = 1; // Indicate failure to shell/orchestrator
        }
    }
}

// --- Run the script ---
main(); 