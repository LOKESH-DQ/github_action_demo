const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

// Import utilities with safe fallbacks
const { 
  extractColumnsFromSQL = () => [], 
  getFileContent = () => null, 
  extractColumnsFromYML = () => [] 
} = require("./sql-parser") || {};

// Get inputs with defaults
const clientId = core.getInput("api_client_id") || "";
const clientSecret = core.getInput("api_client_secret") || "";
const changedFilesList = core.getInput("changed_files_list") || "";
const githubToken = core.getInput("GITHUB_TOKEN") || "";
const dqlabs_base_url = core.getInput("dqlabs_base_url") || "";

// Safe array processing utility
const safeArray = (maybeArray) => Array.isArray(maybeArray) ? maybeArray : [];

const getChangedFiles = async () => {
  try {
    if (changedFilesList && typeof changedFilesList === "string") {
      return changedFilesList
        .split(",")
        .map(f => typeof f === "string" ? f.trim() : "")
        .filter(f => f && f.length > 0);
    }

    const eventPath = process.env.GITHUB_EVENT_PATH;
    if (!eventPath) return [];

    const eventData = JSON.parse(fs.readFileSync(eventPath, "utf8"));
    const changedFiles = new Set();

    const commits = safeArray(eventData.commits);
    commits.forEach(commit => {
      if (!commit) return;
      const files = [
        ...safeArray(commit.added),
        ...safeArray(commit.modified),
        ...safeArray(commit.removed)
      ];
      files.filter(Boolean).forEach(file => changedFiles.add(file));
    });

    return Array.from(changedFiles);
  } catch (error) {
    core.error(`[getChangedFiles] Error: ${error.message}`);
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
      date_filter: { days: "All", selected: "All" },
      chart_filter: {},
      is_chart: true,
    };

    const response = await axios.post(taskUrl, payload, {
      headers: {
        "Content-Type": "application/json",
        "client-id": clientId,
        "client-secret": clientSecret,
      }
    });

    return response?.data?.response?.data || [];
  } catch (error) {
    core.error(`[getTasks] Error: ${error.message}`);
    return [];
  }
};

const getLineageData = async (asset_id, connection_id, entity) => {
  try {
    const lineageUrl = `${dqlabs_base_url}/api/lineage/entities/linked/`;
    const payload = {
      asset_id,
      connection_id,
      entity,
    };
 
    const response = await axios.post(
      lineageUrl,
      payload,
      {
        headers: {
          "Content-Type": "application/json",
          "client-id": clientId,
          "client-secret": clientSecret,
        },
      }
    );
 
    return safeArray(response?.data?.response?.data);
  } catch (error) {
    core.error(`[getLineageData] Error for ${entity}: ${error.message}`);
    return [];
  }
};

const run = async () => {
  try {
    // Initialize summary with basic info
    let summary = "## Impact Analysis Report\n\n";
    
    // Get changed files safely
    const changedFiles = safeArray(await getChangedFiles());
    core.info(`Found ${changedFiles.length} changed files`);
    
    // Process changed SQL models
    const changedModels = changedFiles
      .filter(file => file && typeof file === "string" && file.endsWith(".sql"))
      .map(file => path.basename(file, path.extname(file)))
      .filter(Boolean);

    // Get tasks safely
    const tasks = await getTasks();
    if (!tasks || tasks.length === 0) {
      summary += "No tasks found.\n";
    } else {
      summary += `Found ${tasks.length} tasks.\n`;
    }
    core.info(`Found ${tasks.length} tasks`);

    // Match tasks with changed models
    const matchedTasks = tasks
      .filter(task => task?.connection_type === "dbt")
      .filter(task => changedModels.includes(task?.name))
      .map(task => ({
        ...task,
        entity: task?.task_id || ""
      }));

    for (const task of matchedTasks) {
      summary += `\n### Matched Task\n`;
      summary += `- Task: ${task?.name || 'Unknown'}\n`;
      summary += `  - ID: ${task?.task_id || 'Unknown'}\n`;
      summary += `  - Connection: ${task?.connection_id || 'Unknown'}\n`;
      summary += `  - Asset: ${task?.asset_id || 'Unknown'}\n`;
      summary += `  - Entity: ${task?.entity || 'Unknown'}\n`;
    }

    core.info(`Matched ${matchedTasks.length} tasks with changed models`);

    // Process lineage data
    const Everydata = new Map();

// Initialize data structure for each task
    for (const task of matchedTasks) {
      Everydata.set(task, {
        direct: [],
        indirect: []
      });
    }

    // Process direct lineage for each task
    for (const task of matchedTasks) {
      const lineageTables = await getLineageData(
        task.asset_id,
        task.connection_id,
        task.entity
      );

      const lineageData = safeArray(lineageTables)
        .filter(table => table?.flow === "downstream")
        .filter(Boolean);

      // Store direct impacts for this specific task
      const taskData = Everydata.get(task);
      taskData.direct.push(...lineageData);
    }

    // Process indirect impacts
    const processIndirectImpacts = async (items, task, isFirstLevel = true) => {
      for (const item of safeArray(items)) {
        const lineageTables = await getLineageData(
          item.asset_id,
          item.connection_id,
          item.entity
        );

        // Only add to indirect if it's not the first level (direct impacts)
        if (!isFirstLevel) {
          const taskData = Everydata.get(task);
          taskData.indirect.push(item);
        }

        if (lineageTables && lineageTables.length > 0) {
          const downstream = safeArray(lineageTables)
            .filter(table => table?.flow === "downstream")
            .filter(Boolean);
          await processIndirectImpacts(downstream, task, false);
        }
      }
    };

    // Process indirect impacts for each task's direct impacts
    for (const task of matchedTasks) {
      const taskData = Everydata.get(task);
      await processIndirectImpacts(taskData.direct, task, true);
    }
    // Build summary
    // Build summary
    let totalDirect = 0;
    let totalIndirect = 0;

    // First count totals across all tasks
    for (const [task, impacts] of Everydata) {
      totalDirect += impacts.direct.length;
      totalIndirect += impacts.indirect.length;
    }

    const totalImpacted = totalDirect + totalIndirect;
    summary += `**Total Potential Impact:** ${totalImpacted} downstream items\n`;
    summary += `**Changed Models:** ${changedModels.length}\n\n`;

    // Show impacts per task
    for (const [task, impacts] of Everydata) {
      summary += `## Impacts for Task: ${task.name || 'Unknown'}\n`;
      
      summary += `### Directly Impacted (${impacts.direct.length})\n`;
      if (impacts.direct.length > 0) {
        impacts.direct.forEach(model => {
          summary += `- ${model?.name || 'Unknown'}\n`;
        });
      } else {
        summary += `No direct impacts found\n`;
      }

      summary += `\n### Indirectly Impacted (${impacts.indirect.length})\n`;
      if (impacts.indirect.length > 0) {
        impacts.indirect.forEach(model => {
          summary += `- ${model?.name || 'Unknown'}\n`;
        });
      } else {
        summary += `No indirect impacts found\n`;
      }
      
      summary += `\n`;
    }

    // Optional: Show combined view as well
    summary += `## Combined Impact Summary\n`;
    summary += `**Total Direct Impacts:** ${totalDirect}\n`;
    summary += `**Total Indirect Impacts:** ${totalIndirect}\n\n`;

    // Combined direct impacts
    const allDirect = [];
    const allIndirect = [];

    for (const [_, impacts] of Everydata) {
      allDirect.push(...impacts.direct);
      allIndirect.push(...impacts.indirect);
    }

    // Remove duplicates if needed
    const uniqueDirect = [...new Map(allDirect.map(item => [item.name, item])).values()];
    const uniqueIndirect = [...new Map(allIndirect.map(item => [item.name, item])).values()];

    summary += `### Unique Directly Impacted Models (${uniqueDirect.length})\n`;
    uniqueDirect.forEach(model => {
      summary += `- ${model?.name || 'Unknown'}\n`;
    });

    summary += `\n### Unique Indirectly Impacted Models (${uniqueIndirect.length})\n`;
    uniqueIndirect.forEach(model => {
      summary += `- ${model?.name || 'Unknown'}\n`;
    });
    // Process column changes
    const processColumnChanges = async (extension, extractor) => {
      const changes = [];
      let added = [];
      let removed = [];

      for (const file of changedFiles.filter(f => f && f.endsWith(extension))) {
        try {
          const baseSha = process.env.GITHUB_BASE_SHA || github.context.payload.pull_request?.base?.sha;
          const headSha = process.env.GITHUB_HEAD_SHA || github.context.payload.pull_request?.head?.sha;
          
          const baseContent = baseSha ? await getFileContent(baseSha, file) : null;
          const headContent = await getFileContent(headSha, file);
          if (!headContent) continue;

          const baseCols = safeArray(baseContent ? extractor(baseContent) : []);
          const headCols = safeArray(extractor(headContent));

          const addedCols = headCols.filter(col => !baseCols.includes(col));
          const removedCols = baseCols.filter(col => !headCols.includes(col));

          if (addedCols.length > 0 || removedCols.length > 0) {
            changes.push({ file, added: addedCols, removed: removedCols });
            added.push(...addedCols);
            removed.push(...removedCols);
          }
        } catch (error) {
          core.error(`Error processing ${file}: ${error.message}`);
        }
      }

      return { changes, added, removed };
    };

    // Process SQL changes
    const { added: sqlAdded, removed: sqlRemoved } = await processColumnChanges(".sql", extractColumnsFromSQL);
    summary += `\n### SQL Column Changes\n`;
    summary += `Added: ${sqlAdded.length} columns\n`;
    summary += `Removed: ${sqlRemoved.length} columns\n`;

    // Process YML changes
    const { added: ymlAdded, removed: ymlRemoved } = await processColumnChanges(".yml", extractColumnsFromYML);
    summary += `\n### YML Column Changes\n`;
    summary += `Added: ${ymlAdded.length} columns\n`;
    summary += `Removed: ${ymlRemoved.length} columns\n`;

    // Post comment
    if (github.context.payload.pull_request) {
      try {
        const octokit = github.getOctokit(githubToken);
        await octokit.rest.issues.createComment({
          owner: github.context.repo.owner,
          repo: github.context.repo.repo,
          issue_number: github.context.payload.pull_request.number,
          body: summary,
        });
      } catch (error) {
        core.error(`Failed to create comment: ${error.message}`);
      }
    }

    // Output results
    await core.summary
      .addRaw(summary)
      .write();
      
    core.setOutput("impact_markdown", summary);
  } catch (error) {
    core.setFailed(`[MAIN] Unhandled error: ${error.message}`);
    core.error(error.stack);
  }
};

// Execute
run().catch(error => {
  core.setFailed(`[UNCAUGHT] Critical failure: ${error.message}`);
});