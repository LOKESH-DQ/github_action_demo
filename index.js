const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

// 👇 Import the SQL parser utilities
const { extractColumnsFromSQL, getFileContent } = require("./sql-parser");

const clientId = core.getInput("api_client_id");
const clientSecret = core.getInput("api_client_secret");
const changedFilesList = core.getInput("changed_files_list"); // comma separated list
const githubToken = core.getInput("GITHUB_TOKEN");

const getChangedFiles = async () => {
  if (changedFilesList) {
    return changedFilesList
      .split(",")
      .map(f => f.trim())
      .filter(f => f.length > 0);
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

const extractColumnsFromYaml = (filePath) => {
  try {
    const content = fs.readFileSync(filePath, "utf8");
    const doc = yaml.load(content);
    const models = doc.models || [];
    const columns = {};

    models.forEach(model => {
      (model.columns || []).forEach(col => {
        if (col.name) {
          columns[col.name] = col;
        }
      });
    });

    return columns;
  } catch (err) {
    console.warn(`Failed to read or parse ${filePath}:`, err.message);
    return {};
  }
};

const compareColumns = (oldCols, newCols) => {
  const added = [];
  const removed = [];
  const modified = [];

  const oldKeys = Object.keys(oldCols);
  const newKeys = Object.keys(newCols);

  for (const key of newKeys) {
    if (!oldCols[key]) {
      added.push(key);
    } else if (JSON.stringify(oldCols[key]) !== JSON.stringify(newCols[key])) {
      modified.push({
        name: key,
        old: oldCols[key],
        new: newCols[key],
      });
    }
  }

  for (const key of oldKeys) {
    if (!newCols[key]) {
      removed.push(key);
    }
  }

  return { added, removed, modified };
};

const run = async () => {
  try {
    const context = github.context;
    const changedFiles = await getChangedFiles();

    const changedModels = changedFiles
      .filter(file => file.endsWith(".yml") || file.endsWith(".sql"))
      .map(file => path.basename(file, path.extname(file)))
      .filter((name, index, self) => name && self.indexOf(name) === index);

    const jobAssets = await getJobAssets();

    const matchedAssets = jobAssets
      .filter(asset =>
        changedModels.includes(asset.name) &&
        asset.connection_type === "dbt"
      )
      .map(asset => ({
        name: asset.name,
        asset_id: asset.asset_id,
        connection_id: asset.connection_id,
        connection_name: asset.connection_name,
      }));

    const downstreamAssetsMap = {};
    for (const asset of matchedAssets) {
      const lineageTables = await getLineageData(asset.asset_id, asset.connection_id);
      const downstream = lineageTables.filter(table => table.flow === "downstream");
      downstream.forEach(table => {
        if (!downstreamAssetsMap[table.connection_name]) {
          downstreamAssetsMap[table.connection_name] = [];
        }
        downstreamAssetsMap[table.connection_name].push(table.name);
      });
    }

    // YAML file column comparison
    const columnChanges = [];
    for (const file of changedFiles.filter(f => f.endsWith(".yml"))) {
      const basePath = path.join("base", file);
      const headPath = file;
      const baseColumns = fs.existsSync(basePath) ? extractColumnsFromYaml(basePath) : {};
      const headColumns = fs.existsSync(headPath) ? extractColumnsFromYaml(headPath) : {};
      const { added, removed, modified } = compareColumns(baseColumns, headColumns);
      if (added.length > 0 || removed.length > 0 || modified.length > 0) {
        columnChanges.push({ file, added, removed, modified });
      }
    }

    // SQL file column comparison
    const sqlColumnChanges = [];
    for (const file of changedFiles.filter(f => f.endsWith(".sql"))) {
      const baseSha = process.env.GITHUB_BASE_SHA || github.event.pull_request.base.sha;
      const headSha = process.env.GITHUB_HEAD_SHA || github.event.pull_request.head.sha;
      const baseContent = getFileContent(baseSha, file);
      const headContent = getFileContent(headSha, file);

      if (!headContent) continue;

      const baseCols = baseContent ? extractColumnsFromSQL(baseContent) : [];
      const headCols = extractColumnsFromSQL(headContent);

      const added = headCols.filter(col => !baseCols.includes(col));
      console.log("added", added);
      const removed = baseCols.filter(col => !headCols.includes(col));
      console.log("removed", removed);

      if (added.length > 0 || removed.length > 0) {
        sqlColumnChanges.push({ file, added, removed });
      }
    }

    let summary = `🧠 **Impact Analysis Summary**\n\n`;

    for (const file of changedFiles.filter(f => f.endsWith(".sql"))) {
      const baseSha = process.env.GITHUB_BASE_SHA || github.event.pull_request.base.sha;
      const headSha = process.env.GITHUB_HEAD_SHA || github.event.pull_request.head.sha;
      const baseContent = getFileContent("base", file);
      const headContent = getFileContent("HEAD", file);
      const ref = "HEAD";
      const ref2 = "base";
      summary += `head is ${headSha}\n`;
      summary += `base is ${baseSha}\n`;
      summary += `file is ${file}\n`;

      if (!headContent) continue;
      if (baseContent) {
        summary += `baseContent is these \n`;
      }else {
        summary += `baseContent is not these \n`;
      }

      const baseCols = baseContent ? extractColumnsFromSQL(baseContent) : [];
      summary += `base columns : ${baseCols.join(", ")}\n`;
      const headCols = extractColumnsFromSQL(headContent);
      summary += `head columns : ${headCols.join(", ")}\n`;

      const added = headCols.filter(col => !baseCols.includes(col));
      summary += `added columns : ${added.join(", ")}\n`;
      const removed = baseCols.filter(col => !headCols.includes(col));
      summary += `removed columns : ${removed.join(", ")}\n`;

      if (added.length > 0 || removed.length > 0) {
        sqlColumnChanges.push({ file, added, removed });
      }
    }

    summary += `\n📄 **Changed DBT Models:**\n`;
    if (changedModels.length === 0) {
      summary += `- None\n`;
    } else {
      changedModels.forEach(model => {
        summary += `- ${model}\n`;
      });
    }

    summary += `\n🔗 **Downstream Assets:**\n`;
    if (Object.keys(downstreamAssetsMap).length === 0) {
      summary += `- None found\n`;
    } else {
      for (const [conn, assets] of Object.entries(downstreamAssetsMap)) {
        summary += `- ${conn}:\n`;
        assets.forEach(name => {
          summary += `  - ${name}\n`;
        });
      }
    }

    if (columnChanges.length > 0) {
      summary += `\n🧬 **YAML Column-Level Changes:**\n`;
      summary += `changed columns are b: ${columnChanges}\n`;
      for (const change of columnChanges) {
        summary += `\n- \`${change.file}\`\n`;
        if (change.added.length > 0) {
          summary += `  - ➕ Added: ${change.added.join(", ")}\n`;
        }
        if (change.removed.length > 0) {
          summary += `  - ➖ Removed: ${change.removed.join(", ")}\n`;
        }
        if (change.modified.length > 0) {
          summary += `  - ✏️ Modified Columns:\n`;
          change.modified.forEach(mod => {
            summary += `    - ${mod.name}\n`;
            summary += `      - old: ${JSON.stringify(mod.old)}\n`;
            summary += `      - new: ${JSON.stringify(mod.new)}\n`;
          });
        }
      }
    }

    if (sqlColumnChanges.length > 0) {
      summary += `\n🧾 **SQL Column Changes:**\n`;
      for (const change of sqlColumnChanges) {
        summary += `\n- \`${change.file}\`\n`;
        if (change.added.length > 0) {
          summary += `  - ➕ Added: ${change.added.join(", ")}\n`;
        }
        if (change.removed.length > 0) {
          summary += `  - ➖ Removed: ${change.removed.join(", ")}\n`;
        }
      }
    }

    console.log(summary);

    const octokit = github.getOctokit(githubToken);

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

    await core.summary.addRaw(summary).write();

    core.setOutput("impact_markdown", summary);
    core.setOutput("downstream_assets", JSON.stringify(downstreamAssetsMap));
  } catch (error) {
    core.setFailed(`Error: ${error.message}`);
  }
};

run();
