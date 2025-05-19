const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

const { extractColumnsFromSQL, getFileContent } = require("./sql-parser");

const clientId = core.getInput("api_client_id");
const clientSecret = core.getInput("api_client_secret");
const changedFilesList = core.getInput("changed_files_list");
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
  commits.forEach(commit => {
    [...commit.added, ...commit.modified, ...commit.removed].forEach(file => {
      changedFiles.add(file);
    });
  });

  return Array.from(changedFiles);
};

const getTasks = async () => {
  const taskUrl = "http://44.238.88.190:8000/api/pipeline/task/";
  const payload = {
    chartType: 0,
    search: {},
    page: 0,
    pageLimit: 100,
    sortBy: "name",
    orderBy: "asc",
    date_filter: { days: "All", selected: "All" },
    chart_filter: {},
    is_chart: true
  };

  const response = await axios.post(taskUrl, payload, {
    headers: {
      "client-id": clientId,
      "client-secret": clientSecret,
    },
  });

  return response.data.response.data;
};

// ✅ NEW: fetch downstream models using GET
const fetchDownstreams = async (asset_id, connection_id, entity) => {
  const url = `http://44.238.88.190:8000/api/lineage/?asset_id=${asset_id}&connection_id=${connection_id}&entity=${entity}`;
  const response = await axios.get(url, {
    headers: {
      "client-id": clientId,
      "client-secret": clientSecret,
    }
  });

  return response.data.downstream_models || [];
};

const buildModelKey = (model) => `${model.asset_id}_${model.connection_id}_${model.entity}`;

// ✅ NEW: recursively collect downstreams
const exploreDownstreams = async (asset_id, connection_id, entity, modelMap = {}) => {
  const downstreams = await fetchDownstreams(asset_id, connection_id, entity);

  for (const model of downstreams) {
    const key = buildModelKey(model);
    if (!modelMap[key]) {
      modelMap[key] = model;
      await exploreDownstreams(model.asset_id, model.connection_id, model.entity, modelMap);
    }
  }

  return modelMap;
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
      .map(file => {
        const parts = file.split(path.sep);
        const model = path.basename(file, path.extname(file));
        const job = parts.length >= 2 ? parts[parts.length - 2] : null;
        return { job, model };
      });

    const tasks = await getTasks();

    const matchedTasks = tasks
      .filter(task =>
        task.connection_type === "dbt" &&
        changedModels.some(cm =>
          cm.model === task.name && cm.job === task.job_name
        )
      )
      .map(task => ({
        name: task.name,
        asset_id: task.asset_id,
        connection_id: task.connection_id,
        connection_name: task.connection_name,
        entity: task.task_id,
      }));

    const allImpactedModels = {};

    for (const task of matchedTasks) {
      const modelMap = await exploreDownstreams(task.asset_id, task.connection_id, task.entity);
      const conn = task.connection_name;
      if (!allImpactedModels[conn]) allImpactedModels[conn] = [];
      Object.values(modelMap).forEach(model => {
        if (!allImpactedModels[conn].includes(model.name)) {
          allImpactedModels[conn].push(model.name);
        }
      });
    }

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

    let summary = `🧠 **Impact Analysis Summary**\n\n`;

    for (const file of changedModels) {
      summary += `\n**Changed DBT Model:** \`${file.model}\`\n`;
      summary += `- Job: \`${file.job}\`\n`;
    }

    const sqlColumnChanges = [];
    for (const file of changedFiles.filter(f => f.endsWith(".sql"))) {
      const baseSha = process.env.GITHUB_BASE_SHA || github.context.payload.pull_request?.base?.sha;
      const baseContent = getFileContent(baseSha, file);
      const headSha = process.env.GITHUB_HEAD_SHA || github.context.payload.pull_request?.head?.sha;
      const headContent = getFileContent(headSha, file);

      if (!headContent) continue;

      const baseCols = baseContent ? extractColumnsFromSQL(baseContent) : [];
      const headCols = extractColumnsFromSQL(headContent);

      const added = headCols.filter(col => !baseCols.includes(col));
      const removed = baseCols.filter(col => !headCols.includes(col));

      if (added.length > 0 || removed.length > 0) {
        sqlColumnChanges.push({ file, added, removed });
      }
    }

    summary += `\n🔗 **Impacted Downstream Models (Recursive):**\n`;
    if (Object.keys(allImpactedModels).length === 0) {
      summary += `- None found\n`;
    } else {
      for (const [conn, assets] of Object.entries(allImpactedModels)) {
        summary += `- ${conn}:\n`;
        assets.forEach(name => {
          summary += `  - ${name}\n`;
        });
      }
    }

    if (columnChanges.length > 0) {
      summary += `\n🧬 **YAML Column-Level Changes:**\n`;
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
    core.setOutput("downstream_assets", JSON.stringify(allImpactedModels));
  } catch (error) {
    core.setFailed(`Error: ${error.message}`);
  }
};

run();
