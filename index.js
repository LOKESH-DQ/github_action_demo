const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

const clientId = core.getInput("api_client_id");
const clientSecret = core.getInput("api_client_secret");
const changedFilesList = core.getInput("changed_files_list");  // Optional
const githubToken = core.getInput("GITHUB_TOKEN");

const getChangedFiles = async () => {
  if (changedFilesList) {
    return changedFilesList.split(",").map(f => f.trim());
  }

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

const getJobAssets = async () => {
  const jobUrl = "http://44.238.88.190:8000/api/pipeline/job/";
  const response = await axios.post(
    jobUrl,
    {},
    {
      headers: {
        "client-id": clientId,
        "client-secret": clientSecret,
      },
    }
  );
  return response.data.response.data;
};

const getLineageData = async (asset_id, connection_id) => {
  const lineageUrl = "http://44.238.88.190:8000/api/lineage/";
  const body = {
    asset_id,
    connection_id,
    entity: asset_id,
  };

  const response = await axios.post(
    lineageUrl,
    body,
    {
      headers: {
        "client-id": clientId,
        "client-secret": clientSecret,
      },
    }
  );

  return response.data.response.data.tables;
};

const run = async () => {
  try {
    const eventPath = process.env.GITHUB_EVENT_PATH;
    const eventData = JSON.parse(fs.readFileSync(eventPath, "utf8"));
    const commits = eventData.commits || [];

    const changedFiles = await getChangedFiles();

    const changedModels = changedFiles
      .filter((file) => file.endsWith(".yml") || file.endsWith(".sql"))
      .map((file) => path.basename(file, path.extname(file)))
      .filter((name, index, self) => name && self.indexOf(name) === index);

    const jobAssets = await getJobAssets();

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
      }));

    const downstreamAssets = [];

    for (const asset of matchedAssets) {
      const lineageTables = await getLineageData(asset.asset_id, asset.connection_id);
      const downstream = lineageTables.filter((table) => table.flow === "downstream");

      downstream.forEach((table) => {
        downstreamAssets.push({
          name: table.name,
          connection_name: table.connection_name,
        });
      });
    }

    // Build markdown summary
    let summary = `🧠 **Impact Analysis Summary**\n\n`;

    // Print filtered changed models
    summary += `\n📄 **Changed DBT Models:**\n`;
    if (changedModels.length === 0) {
      summary += `- None\n`;
    } else {
      changedModels.forEach((model) => {
        summary += `- ${model}\n`;
      });
    }

    // Print downstream assets
    summary += `\n🔗 **Downstream Assets:**\n`;
    if (downstreamAssets.length === 0) {
      summary += `- None found\n`;
    } else {
      downstreamAssets.forEach((asset) => {
        summary += `- ${asset.name} (${asset.connection_name})\n`;
      });
    }

    console.log(summary);

    // Post comment on PR (if PR context exists)
    const octokit = github.getOctokit(githubToken);
    const context = github.context;

    if (context.payload.pull_request) {
      await octokit.rest.issues.createComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: context.payload.pull_request.number,
        body: summary,
      });
    } else {
      core.info("No pull request found in the context, skipping comment post.");
    }

    // Write summary to GitHub Actions UI
    await core.summary
      .addRaw(summary)
      .write();

    // Set outputs
    core.setOutput("impact_markdown", summary);
    core.setOutput("downstream_assets", JSON.stringify(downstreamAssets));

  } catch (error) {
    core.setFailed(`Error: ${error.message}`);
  }
};

run();
