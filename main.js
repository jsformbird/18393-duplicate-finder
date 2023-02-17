"use strict";
const fs = require("fs");
/**
 * An over simplified mongo provider intended to be used for preparint the test db.
 * For code readability, We won't use the the formbird platform's mongodb provider because it has logic
 * we don't necessarily need for setting up the test db.
 */
const MongoClient = require("mongodb").MongoClient;

//------- DEFAULTS HERE ----
let A_SRC_URI;
let A_TARGET_URI;

let A_TARGETDB = 'gww-uat';
let A_TARGETHOST = '10.201.1.20';
let A_TARGETPORT = '27017';
let A_TARGETUSERNAME = 'dev';
let A_TARGETAUTHDB = 'admin';
let A_TARGETPASS = '****';

let A_SRCDB = 'gww-uat-cutover';
let A_SRCHOST = '10.201.1.20';
let A_SRCPORT = '27017';
let A_SRCUSERNAME = 'dev';
let A_SRCAUTHDB = 'admin';
let A_SRCPASS = '****';

let A_TARGET_RESTORE_DATE;
let A_QUERY = `{"$and":[{"appTags":{"$ne": {"$in": ["directScript","rulesetinclude","ruleset"]}}},{"systemHeader.systemType": {"$ne": {"$in":["configuration","ruleset","rulesetInclude","template","component"]}}}, {"$or":[{"systemHeader.serverUpdatedDate" : {"$gte": {"date": targetRestoreDate}}}, {"documentId":"a3426c80-d5e3-11e5-bb4c-0f0be17ce808"}, {"systemHeader.systemType":"schedule"}]}]}`;
// let A_QUERY = `{ }`;
let A_FILENAME;

let A_TARGETDB_SAME_AS_SOURCE = false;


for (const a of process.argv) {
  // const val = a.trim().split('=')[1]?.trim();
  const val = a.slice(a.indexOf('=') + 1).trim();
  if (hasStr('targetdb', a)) {
    A_TARGETDB = val;
  } else if (hasStr('targetdb', a)) {
    A_TARGETDB = val;
  } else if (hasStr('targethost', a)) {
    A_TARGETHOST = val;
  } else if (hasStr('targetport', a)) {
    A_TARGETPORT = val;
  } else if (hasStr('targetusername', a)) {
    A_TARGETUSERNAME = val;
  } else if (hasStr('targetauthenticationdb', a)) {
    A_TARGETAUTHDB = val;
  } else if (hasStr('targetpassword', a)) {
    A_TARGETPASS = encodeURIComponent(val);
  } else if (hasStr('sourcedb', a)) {
    A_SRCDB = val;
  } else if (hasStr('sourcehost', a)) {
    A_SRCHOST = val;
  } else if (hasStr('sourceport', a)) {
    A_SRCPORT = val;
  } else if (hasStr('sourceusername', a)) {
    A_SRCUSERNAME = val;
  } else if (hasStr('sourceauthenticationdb', a)) {
    A_SRCAUTHDB = val;
  } else if (hasStr('sourcepassword', a)) {
    A_SRCPASS = encodeURIComponent(val);
  } else if (hasStr('--targetRestoreDate', a)) {
    const dateVal = new Date(val);
    if (isNaN(dateVal)) {
      throw new Error('Invalid target restore date');
    }
    A_TARGET_RESTORE_DATE = dateVal;
  } else if (hasStr('filename', a)) {
    A_FILENAME = val;
  } else if (hasStr('tsas', a)) {
    A_TARGETDB_SAME_AS_SOURCE = true;
  } else if (hasStr('query', a)) {
    const valStr = val.replaceAll("", '');
    A_QUERY = valStr;
  } else if (hasStr('--src', a)) {
    A_SRC_URI = val;
  } else if (hasStr('--target', a)) {
    A_TARGET_URI = val;
  }
  function hasStr(str, target) {
    return target.indexOf(str) >= 0;
  }
}

async function getDb(config) {
  // mongodb://Admin:${DBPASSWORD}@<host>:<port>/admin?authSource=admin
  // https://www.mongodb.com/docs/manual/reference/connection-string/#connections-connection-options
  let uri;
  if (config.uri){
    uri = config.uri;
  } else {
    const usernamepass = config.username && config.pass ? `${config.username}:${config.pass}@` : '';
    const authdb = config.username && config.authdb ? `?authSource=${config.authdb}` : '';
    uri = `mongodb://${usernamepass}${config.host}:${config.port}/${config.db}${authdb}`;
  }
  console.log(`Connecting to: ${uri}`);
  const client = await MongoClient.connect(uri, { useNewUrlParser: true });
  return config.uri ? client.db() : client.db(config.db);
};

async function findDocuments(db, query, sort) {
  const collection = db.collection('documents');
  let cursor = collection.find(query);
  const arr = await cursor.toArray();
  return arr;
};

function getTargetDb() {
  const tarConfig = {
    username: A_TARGETUSERNAME,
    pass: A_TARGETPASS,
    authdb: A_TARGETAUTHDB,
    db: A_TARGETDB,
    port: A_TARGETPORT,
    host: A_TARGETHOST,
    uri: A_TARGET_URI
  };
  return getDb(tarConfig);
}

function removeDocuments (db, filter) {
  const col = db.collection('documents');
// .remove(query);
  return col.deleteMany(filter, {});
};

async function main() {
  if (A_FILENAME) {
    return removeDuplicatesFromCSVFile();
  }
  return detectConflictsAndWriteToCSVFile();
}

async function removeDuplicatesFromCSVFile() {
  if (!A_TARGETDB) {
    throw new Error('Target database is required when removing versions listed in a file.');
  }

  console.log(`Reading csv:`);
  console.log(A_FILENAME);
  const csvStr = await new Promise((resolve, reject) =>  {
    const fs = require("fs");
    fs.readFile(A_FILENAME, "utf-8", (err, data) => {
      if (err) reject(err);
      else resolve(data);
    });
  });

  const lines = csvStr.split('\n');
  let rowCount = 0;
  let toRemoveVersions = []
  for (const line of lines) {
    if (rowCount++ === 0) {
      continue; // skip header
    }
    if (!line.trim()) { // ignore empty lines
      continue;
    }
    const cols = line.split(',');
    if (cols.length !== 5) {
      throw new Error(`Line with index ${rowCount} has ${cols.length} columns instead of the expected 5`);
    }
    const versionId = cols[4];
    toRemoveVersions.push(versionId);
  }
  const toRemoveQuery = {'systemHeader.versionId': {$in: toRemoveVersions} };

  const readline = require('readline').createInterface({
    input: process.stdin,
    output: process.stdout
  });
  readline.question(`Type "proceed-with-remove" proceed with removing ${toRemoveVersions.length} versions a target: `, async (response) => {
    if (response.toLowerCase().trim() === 'proceed-with-remove') {
      const targetdb = await getTargetDb();
      const result = await removeDocuments(targetdb, toRemoveQuery)
      console.log(`Operation response from db: ${JSON.stringify(result, null, 4)}`);
    } else {
      console.log('Aborted remove operation.');
    }
    readline.close();
    process.exit();
  });

}
async function detectConflictsAndWriteToCSVFile() {
  if (!A_TARGET_RESTORE_DATE) {
    throw new Error('Target restore date not set. Set as app param as follows: "--targetRestoreDate=2021-01-27T06:55:45.366Z"');
  }

  let evaluatedSrcQuery;
  try {
    // eslint-disable-next-line max-len
    const getQuery = eval(`(targetRestoreDate) => { return ${A_QUERY} }`);
    evaluatedSrcQuery = getQuery(A_TARGET_RESTORE_DATE);
    console.log(`Final source query:\n${JSON.stringify(evaluatedSrcQuery, null, 0)}\n`);
  } catch (e) {
    console.error(e);
    throw new Error('An error occured while evaluating query: ' + e.message);
  }

  const srcConfig = {
    username: A_SRCUSERNAME,
    pass: A_SRCPASS,
    authdb: A_SRCAUTHDB,
    db: A_SRCDB,
    port: A_SRCPORT,
    host: A_SRCHOST,
    uri: A_SRC_URI
  };
  const srcDb = await getDb(srcConfig);
  let targetDb;
  if (A_TARGETDB_SAME_AS_SOURCE) {
    targetDb = srcDb;
  } else {
    targetDb = await getTargetDb();
  }
  const toProcess = await findDocuments(srcDb, evaluatedSrcQuery);
  // const toProcess = src;
  const documentIds = new Set();
  const toRemoveVersion = [];
  let toRemoveDocs = [];

  let docsWithUpdatesToRemoveCount = 0;

  for (const doc of toProcess) {
    // record unique documentIds
    documentIds.add(doc.documentId);
    const versionId = doc.systemHeader.versionId;

    const docsAtTarget = await getDocAtTarget(versionId);
    if (!docsAtTarget){
      continue;
    }
    for (const d of docsAtTarget) {
      if (d.systemHeader.currentVersion === doc.systemHeader.currentVersion){
        toRemoveVersion.push(d);
      }
    }

  }

  console.log(`Source counts:\n documents: ${documentIds.size}\n versions: ${toProcess.length}`);
  console.log(`Duplicate versions to remove from target: ${toRemoveVersion.length}`);
  for (const docId of documentIds) {
    //  find documents in the target that were created after the restore date
    const updateDocs = await getDocUpdatesAtTarget(docId, A_TARGET_RESTORE_DATE);
    if ( updateDocs && updateDocs.length > 0) {
      docsWithUpdatesToRemoveCount++;
      toRemoveDocs = toRemoveDocs.concat(updateDocs);
    }
  }

  console.log(`Updates to be removed from target:\n documents: ${docsWithUpdatesToRemoveCount}\n versions: ${toRemoveDocs.length}`);
  const conflicts = {toRemoveVersion, toRemoveDocs};
  return conflictsToFile(conflicts);

  async function getDocAtTarget(versionId){
    const result = await findDocuments(targetDb, {'systemHeader.versionId' : versionId });
    if (result && result.length === 1) {
      return result;
    } else if (result.length > 1) {
      console.error(`Multiple documents found at target with versionId: ${versionId}.`);
    }
    return null;
  }

  async function getDocUpdatesAtTarget(documentId, targetRestoreDate) {
    // eslint-disable-next-line max-len
    const result = await findDocuments(targetDb, {'documentId' : documentId, "systemHeader.serverUpdatedDate" : {"$gte": {"date": targetRestoreDate}}});
    return result && result.length > 0 ? result : [];
  }

  async function conflictsToFile(result) {
    const map = new Map();
    for (const a of result.toRemoveVersion) {
      if (!map.has(a._id)) {
        a.__duplicateType = 'versionId';
        map.set(a._id, a);
      }
    }
    for (const a of result.toRemoveDocs) {
      if (!map.has(a._id)) {
        a.__duplicateType = 'documentId';
        map.set(a._id, a);
      }
    }

    const readline = require('readline').createInterface({
      input: process.stdin,
      output: process.stdout
    });
    readline.question('Type c to continue writing to csv: ', (response) => {
      if (response.toLowerCase().trim() === 'c') {
        generateCSVFile();
      } else {
        console.log('Aborted writing to csv.');
      }
      readline.close();
      process.exit();
    });

    function generateCSVFile() {


      // console.log('\nTo remove documents at target (csv) ->');
      let csv = 'duplicateType,summaryName,serverUpdatedDate,documentId,versionId\n';
      for (const a of map.values()) {
        // type, systemHeader.summaryName, systemHeader.serverUpdatedDate, documentId, versionId
        // eslint-disable-next-line max-len
        const dateStr = a.systemHeader.serverUpdatedDate ? new Date(a.systemHeader.serverUpdatedDate).toISOString() : undefined;
        csv += `${a.__duplicateType},${a.systemHeader.summaryName?.replaceAll(',', ' ')},${ dateStr },${a.documentId},${a.systemHeader.versionId}\n`;
      }
      // console.log(csv);

      const fs = require("fs");
      const tzoffset = (new Date()).getTimezoneOffset() * 60000; //offset in milliseconds
      const restoreDateAslocalISOTime = (new Date(A_TARGET_RESTORE_DATE - tzoffset)).toISOString().slice(0, -1);
      let restoreDateSerialised = restoreDateAslocalISOTime.replaceAll(':', '');
      restoreDateSerialised = restoreDateSerialised.split('.')[0]; // remove digit seconds
      // "targetdb"_doc_conflicts_$(date+'%Y-%M-%d')T$(date+'%H-%M').csv
      const filename = `${targetDb.databaseName}_doc_conflicts_${restoreDateSerialised}.csv`;
      fs.writeFileSync(filename, csv);
      console.log(`File written: ${filename}`);
    }

  }

}

main().then(result => {
  if (result) {
    console.log(result);
  }
}).catch(err => {
  console.log('Uncaught error: ' + err.message ? err.message : err);
  throw err;
});


