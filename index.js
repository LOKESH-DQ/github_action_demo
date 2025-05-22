const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

// Import the SQL parser utilities
const { extractColumnsFromSQL, getFileContent, extractColumnsFromYML } = require("./sql-parser");

const clientId = core.getInput("api_client_id");
const clientSecret = core.getInput("api_client_secret");
const changedFilesList = core.getInput("changed_files_list") || ""; // Ensure it's never undefined
const githubToken = core.getInput("GITHUB_TOKEN");
const dqlabs_base_url = core.getInput("dqlabs_base_url");

const getChangedFiles = async () => {
  try {
    if (changedFilesList) {
      return changedFilesList
        .split(",")
        .map(f => f.trim())
        .filter(f => f.length > 0);
    }

    const eventPath = process.env.GITHUB_EVENT_PATH;
    if (!eventPath) {
      core.info("No GITHUB_EVENT_PATH found");
      return [];
    }

    const eventData = JSON.parse(fs.readFileSync(eventPath, "utf8"));
    const changedFiles = new Set();

    const commits = eventData.commits || [];
    commits.forEach(commit => {
      [...(commit.added || []), ...(commit.modified || []), ...(commit.removed || [])].forEach(file => {
        if (file) changedFiles.add(file);
      });
    });

    return Array.from(changedFiles);
  } catch (error) {
    core.error(`Error in getChangedFiles: ${error.message}`);
    return [];
  }
};

const getTasks = async () => {
  try {
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

    return response?.data?.response?.data || [];
  } catch (error) {
    core.error(`Error in getTasks: ${error.message}`);
    return [];
  }
};

const getLineageData = async (asset_id, connection_id, entity) => {
  try {
    const lineageUrl = `${dqlabs_base_url}/api/lineage/entities/linked/`;
    core.info(`Fetching lineage data from: ${lineageUrl}`);
    const body = { asset_id, connection_id, entity };

    const response = await axios.post(lineageUrl, body, {
      headers: {
        "Content-Type": "application/json",
        "client-id": clientId,
        "client-secret": clientSecret,
      },
    });

    return response?.data?.response?.data?.tables || [];
  } catch (error) {
    core.error(`Error in getLineageData: ${error.message}`);
    return [];
  }
};

const run = async () => {
  try {
    const context = github.context;
    const changedFiles = await getChangedFiles();
    core.info(`Changed files: ${JSON.stringify(changedFiles)}`);

    const changedModels = changedFiles
      .filter(file => file && file.endsWith(".sql"))
      .map(file => path.basename(file, path.extname(file))
      .filter(Boolean);

    const tasks = await getTasks();
    core.info(`Found ${tasks.length} tasks`);

    const matchedTasks = tasks
      .filter(task => 
        task?.connection_type === "dbt" &&
        changedModels.some(cm => cm === task?.name)
      )
      .map(task => ({
        ...task,
        entity: task?.task_id,
      }));

    core.info(`Matched ${matchedTasks.length} tasks with changed models`);

    const Everydata = {
      direct: [],
      indirect: []
    };

    for (const task of matchedTasks) {
      try {
        const lineageTables = await getLineageData(task.asset_id, task.connection_id, task.entity);
        
        if (Array.isArray(lineageTables) && lineageTables.length > 0) {
          const lineageData = lineageTables.filter(table => table?.flow === "downstream");
          Everydata.direct.push(...lineageData);
        }
      } catch (error) {
        core.error(`Error processing task ${task?.name}: ${error.message}`);
      }
    }

    const indirectlyImpactedModels = async (list) => {
      for (const item of list) {
        try {
          const lineageTables = await getLineageData(item.asset_id, item.connection_id, item.entity);
          
          if (!Array.isArray(lineageTables) || lineageTables.length === 0) {
            Everydata.indirect.push(item);
            continue;
          }
          
          const lineageData = lineageTables.filter(table => table?.flow === "downstream");
          Everydata.indirect.push(item);
          await indirectlyImpactedModels(lineageData);
        } catch (error) {
          core.error(`Error processing indirect impact for ${item?.name}: ${error.message}`);
        }
      }
    };

    await indirectlyImpactedModels(Everydata.direct);

    const totalImpacted = Everydata.direct.length + Everydata.indirect.length;
    
    let summary = `\n**Total Potential Impact:** ${totalImpacted} unique downstream items\n`;
    summary += `**Changed Models:** ${changedModels.length}\n`;
    
    summary += `\n**Directly Impacted Models (${Everydata.direct.length}):**\n`;
    Everydata.direct.forEach(model => {
      summary += `- ${model?.name || 'Unknown'}\n`;
    });
    
    summary += `\n**Indirectly Impacted Models (${Everydata.indirect.length}):**\n`;
    Everydata.indirect.forEach(model => {
      summary += `- ${model?.name || 'Unknown'}\n`;
    });

    const sqlColumnChanges = [];
    let allAddedColumns = [];
    let allRemovedColumns = [];

    for (const file of changedFiles.filter(f => f && f.endsWith(".sql"))) {
      try {
        const baseSha = process.env.GITHUB_BASE_SHA || context.payload.pull_request?.base?.sha;
        const headSha = process.env.GITHUB_HEAD_SHA || context.payload.pull_request?.head?.sha;
        const baseContent = baseSha ? await getFileContent(baseSha, file) : null;
        const headContent = await getFileContent(headSha, file);

        if (!headContent) continue;

        const baseCols = baseContent ? (extractColumnsFromSQL(baseContent) || []) : [];
        const headCols = extractColumnsFromSQL(headContent) || [];

        const added = headCols.filter(col => !baseCols.includes(col));
        const removed = baseCols.filter(col => !headCols.includes(col));

        if (added.length > 0 || removed.length > 0) {
          sqlColumnChanges.push({ file, added, removed });
          allAddedColumns.push(...added);
          allRemovedColumns.push(...removed);
        }
      } catch (error) {
        core.error(`Error processing SQL file ${file}: ${error.message}`);
      }
    }

    summary += `\n**Column Changes:**\n`;
    summary += `Added columns (${allAddedColumns.length}): ${allAddedColumns.join(", ")}\n`;
    summary += `Removed columns (${allRemovedColumns.length}): ${allRemovedColumns.join(", ")}\n`;

    const ymlColumnChanges = [];
    let allAddedYmlColumns = [];
    let allRemovedYmlColumns = [];

    for (const file of changedFiles.filter(f => f && f.endsWith(".yml"))) {
      try {
        const baseSha = process.env.GITHUB_BASE_SHA || context.payload.pull_request?.base?.sha;
        const headSha = process.env.GITHUB_HEAD_SHA || context.payload.pull_request?.head?.sha;
        const baseContent = baseSha ? await getFileContent(baseSha, file) : null;
        const headContent = await getFileContent(headSha, file);

        if (!headContent) continue;

        const baseCols = baseContent ? (extractColumnsFromYML(baseContent) || []) : [];
        const headCols = extractColumnsFromYML(headContent) || [];

        const added = headCols.filter(col => !baseCols.includes(col));
        const removed = baseCols.filter(col => !headCols.includes(col));

        if (added.length > 0 || removed.length > 0) {
          ymlColumnChanges.push({ file, added, removed });
          allAddedYmlColumns.push(...added);
          allRemovedYmlColumns.push(...removed);
        }
      } catch (error) {
        core.error(`Error processing YML file ${file}: ${error.message}`);
      }
    }

    summary += `\n**YML Column Changes:**\n`;
    summary += `Added YML columns (${allAddedYmlColumns.length}): ${allAddedYmlColumns.join(", ")}\n`;
    summary += `Removed YML columns (${allRemovedYmlColumns.length}): ${allRemovedYmlColumns.join(", ")}\n`;

    const octokit = github.getOctokit(githubToken);

    if (context.payload.pull_request) {
      try {
        await octokit.rest.issues.createComment({
          owner: context.repo.owner,
          repo: context.repo.repo,
          issue_number: context.payload.pull_request.number,
          body: summary,
        });
      } catch (error) {
        core.error(`Error creating comment: ${error.message}`);
      }
    } else {
      core.info("No pull request found in the context, skipping comment post.");
    }

    await core.summary
      .addRaw(summary)
      .write();
      
    core.setOutput("impact_markdown", summary);
  } catch (error) {
    core.setFailed(`Error in main execution: ${error.message}`);
    core.error(error.stack);
  }
};

run();