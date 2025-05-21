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
const changedFilesList = core.getInput("changed_files_list"); // comma-separated list
const githubToken = core.getInput("GITHUB_TOKEN");
const dqlabs_base_url = core.getInput("dqlabs_base_url");

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
  const taskUrl = `${dqlabs_base_url}/api/pipeline/task/`;
  const payload = {
    chartType: 0,
    search: {},
    page: 0,
    pageLimit: 100,
    sortBy: "name",
    orderBy: "asc",
    date_filter: {
      days: "All",
      selected: "All",
    },
    chart_filter: {},
    is_chart: true,
  };

  const response = await axios.post(taskUrl, payload, {
    headers: {
      "Content-Type": "application/json",
      "client-id": clientId,
      "client-secret": clientSecret,
    },
  });

  return response.data.response.data;
};

const getLineageData = async (asset_id, connection_id, entity) => {
  const lineageUrl = `${dqlabs_base_url}/api/lineage/`;
  core.info("lineage url:", lineageUrl);
  const body = { asset_id, connection_id, entity };

  const response = await axios.post(lineageUrl, body, {
    headers: {
      "Content-Type": "application/json",
      "client-id": clientId,
      "client-secret": clientSecret,
    },
  });

  return response.data.response.data.tables;
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

    const Everydata = {
      direct: [],
      indirect: [],
    };

    const directlyImpactedModels = {};

    for (const task of matchedTasks) {
      const lineageTables = await getLineageData(task.asset_id, task.connection_id, task.entity);
      const lineageData = lineageTables.filter(table => table.flow === "downstream" && table.name !== task.name);

      lineageData.forEach(table => {
        table.modelEntity = task.entity;
        if (!directlyImpactedModels[table.connection_name]) {
          directlyImpactedModels[table.connection_name] = [];
        }
        directlyImpactedModels[table.connection_name].push(table.name);
      });

      Everydata.direct.push(...lineageData);
    }

    const indirectlyImpactedModels = async (list, x, entity) => {
      for (const item of list) {
        const entity_final = x === "task" && entity ? entity : item.modelEntity;
        const lineageTables = await getLineageData(item.asset_id, item.connection_id, entity_final);

        if (lineageTables.length === 0) {
          Everydata.indirect.push(item);
          continue;
        }

        const filtered = lineageTables.filter(table => table.flow === "downstream" && table.name !== item.name);
        Everydata.indirect.push(item);
        await indirectlyImpactedModels(filtered, "task", entity_final);
      }
    };

    await indirectlyImpactedModels(Everydata.direct, "job", "");

    const uniqueModels = new Set();

    [...Everydata.direct, ...Everydata.indirect].forEach(item => {
      if (item.name) uniqueModels.add(item.name);
    });

    let summary = `\n **DQLabs Impact Report**\n`;
    count = uniqueModels.size;
    summary += `\n **Total Potential impact: ${count} unique downstream items across ${changedModels.length} changed Dbt models\n`;
    summary += `\n **Directly Impacted Models:** ${Everydata.direct.length}\n`;

    if (count <= 20) {
      for (const task of Everydata.direct) {
        summary += `     - ${task.name}\n`;
      }
    }
    summary += `\n **Indirectly Impacted Models:** ${Everydata.indirect.length}\n`;
    if (count <= 20) {
      for (const task of Everydata.indirect) {
        summary += `     - ${task.name}\n`;
      }
    }


    const sqlColumnChanges = [];
    let allAddedColumns = [];
    let allRemovedColumns = [];

    for (const file of changedFiles.filter(f => f.endsWith(".sql"))) {
      const baseSha = process.env.GITHUB_BASE_SHA || github.context.payload.pull_request?.base?.sha;
      const headSha = process.env.GITHUB_HEAD_SHA || github.context.payload.pull_request?.head?.sha;
      const baseContent = getFileContent(baseSha, file);
      const headContent = getFileContent(headSha, file);

      if (!headContent) continue;

      const baseCols = baseContent ? extractColumnsFromSQL(baseContent) : [];
      const headCols = extractColumnsFromSQL(headContent);

      const added = headCols.filter(col => !baseCols.includes(col));
      const removed = baseCols.filter(col => !headCols.includes(col));

      if (added.length > 0 || removed.length > 0) {
        sqlColumnChanges.push({ file, added, removed });
        allAddedColumns.push(...added);
        allRemovedColumns.push(...removed);
      }
    }

    summary += `\n **Column Changes:**\n`;
    summary += `added checkcolumns(${allAddedColumns.length}): ${allAddedColumns.join(", ")}\n`;
    summary += `removed checkcolumns(${allRemovedColumns.length}): ${allRemovedColumns.join(", ")}\n`;

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
    core.setOutput("downstream_assets", JSON.stringify(directlyImpactedModels));
  } catch (error) {
    core.setFailed(`Error: ${error.message}`);
  }
};

run();
