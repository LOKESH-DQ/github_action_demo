const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

const clientId = core.getInput("client_id");
const clientSecret = core.getInput("client_secret");

const getChangedFiles = async () => {
  const eventPath = process.env.GITHUB_EVENT_PATH;
  const eventData = JSON.parse(fs.readFileSync(eventPath, "utf8"));
  const changedFiles = new Set();

  const commits = eventData.commits || [];
  commits.forEach((commit) => {
    [...commit.added, ...commit.modified, ...commit.removed].forEach((file) => {
      changedFiles.add(file);
    });
  });

  return Array.from(changedFiles);
};

const getAuthToken = async () => {
  const tokenUrl = "http://44.238.88.190:8000/api/api_token";
  const response = await axios.post(tokenUrl, {
    "client-id": clientId,
    "client-secret": clientSecret,
  });
  return response.data.token;
};

const getJobAssets = async (authToken) => {
  const jobUrl = "http://44.238.88.190:8000/api/pipeline/job/";
  const response = await axios.post(
    jobUrl,
    {},
    // { headers: { Authorization: `Bearer ${authToken}` } }
  );
  return response.data.response.data;
};

const getLineageData = async (authToken, asset_id, connection_id) => {
  const lineageUrl = "http://44.238.88.190:8000/api/lineage/";
  
  const body = {
    asset_id: asset_id,
    connection_id: connection_id,
    entity: asset_id,  // Entity is the same as asset_id
  };

  const response = await axios.post(
    lineageUrl,
    body,
    // { headers: { Authorization: `Bearer ${authToken}` } }  // Authorization in the header
  );
  
  return response.data.response.data.tables;  // Extract tables from the response
};

const run = async () => {
  try {
    const changedFiles = await getChangedFiles();
    console.log("Changed files:", changedFiles);

    // Extract model names from paths like models/customer.yml -> "customer"
    const changedModels = changedFiles
      .filter((file) => file.endsWith(".yml") || file.endsWith(".sql"))  // Check for both .yml and .sql files
      .map((file) => path.basename(file, path.extname(file)));  // Extract the file name without the extension

    console.log("Changed models:", changedModels);

    const authToken = await getAuthToken();
    const jobAssets = await getJobAssets(authToken);

    // Filter relevant job assets
    const matchedAssets = jobAssets
      .filter(
        (asset) =>
          changedModels.includes(asset.name) &&
          asset.connection_type === "dbt"
      )
      .map((asset) => ({
        name: asset.name,
        asset_id: asset.asset_id,
        connection_id: asset.connection_id,
        connection_name: asset.connection_name,
        connection_type: asset.connection_type,
      }));

    console.log("Matched job assets:");
    console.log(JSON.stringify(matchedAssets, null, 2));

    // Get lineage data for each matched asset
    const downstreamAssets = [];

    for (const asset of matchedAssets) {
      const lineageData = await getLineageData(
        authToken,
        asset.asset_id,
        asset.connection_id
      );

      // Filter downstream assets
      const downstream = lineageData.filter(
        (table) => table.flow === "downstream"
      );

      downstream.forEach((lineage) => {
        downstreamAssets.push({
          name: lineage.name,
          connection_name: lineage.connection_name,
        });
      });
    }

    console.log("Downstream assets:");
    console.log(JSON.stringify(downstreamAssets, null, 2));

    core.setOutput("downstream_assets", JSON.stringify(downstreamAssets));
  } catch (error) {
    core.setFailed(`Error: ${error.message}`);
  }
};

run();
