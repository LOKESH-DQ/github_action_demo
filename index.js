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
const dqlabs_createlink_url = core.getInput("dqlabs_createlink_url") || "";

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

const getImpactAnalysisData = async (asset_id, connection_id, entity, isDirect = true) => {
  try {
    const impactAnalysisUrl = `${dqlabs_base_url}/api/lineage/impact-analysis/`;
    const payload = {
      connection_id,
      asset_id,
      entity,
      moreOptions: {
        view_by: "table",
        ...(!isDirect && { depth: 10 })
      },
      search_key: ""
    };

    const response = await axios.post(
      impactAnalysisUrl,
      payload,
      {
        headers: {
          "Content-Type": "application/json",
          "client-id": clientId,
          "client-secret": clientSecret,
        },
      }
    );

    return safeArray(response?.data?.response?.data?.tables || []);
  } catch (error) {
    core.error(`[getImpactAnalysisData] Error for ${entity}: ${error.message}`);
    return [];
  }
};

const constructItemUrl = (item, baseUrl) => {
  if (!item || !baseUrl) return "#";
  
  try {
    const url = new URL(baseUrl);
    
    if (item.asset_group === "pipeline") {
      if (item.is_transform) {
        url.pathname = `/observe/pipeline/transformation/${item.redirect_id}/run`;
      } else {
        url.pathname = `/observe/pipeline/task/${item.redirect_id}/run`;
      }
      return url.toString();
    }
    
    if (item.asset_group === "data") {
      url.pathname = `/observe/data/${item.redirect_id}/measures`;
      return url.toString();
    }
    
    return "#";
  } catch (error) {
    core.error(`Error constructing URL for ${item.name}: ${error.message}`);
    return "#";
  }
};

const run = async () => {
  try {
    let summary = "## Impact Analysis Report\n\n";
    const changedFiles = safeArray(await getChangedFiles());
    summary += "\n ## Changed Files\n";
    changedFiles.forEach(file => {
      summary += `- ${file}\n`;
    });
    core.info(`Found ${changedFiles.length} changed files`);
    
    const changedModels = changedFiles
      .filter(file => file && typeof file === "string" && file.endsWith(".sql"))
      .map(file => path.basename(file, path.extname(file)))
      .filter(Boolean);

    const tasks = await getTasks();
    
    const matchedTasks = tasks
      .filter(task => task?.connection_type === "dbt")
      .filter(task => changedModels.includes(task?.name))
      .map(task => ({
        ...task,
        entity: task?.task_id || "",
        modelName: task?.name || ""
      }));

    const impactsByFile = {};

    for (const task of matchedTasks) {
      const fileKey = `${task.modelName}.sql`;
      
      const directImpact = await getImpactAnalysisData(
        task.asset_id,
        task.connection_id,
        task.entity,
        true
      );

      const filteredDirectImpact = directImpact
        .filter(table => table?.name !== task.modelName)
        .filter(Boolean);

      const indirectImpact = await getImpactAnalysisData(
        task.asset_id,
        task.connection_id,
        task.entity,
        false
      );

      const directKeys = new Set(filteredDirectImpact.map(item => `${item?.name}-${item?.connection_id}-${item?.asset_name}`));
      const filteredIndirectImpact = indirectImpact.filter(
        item => !directKeys.has(`${item?.name}-${item?.connection_id}-${item?.asset_name}`)
      );

      const dedup = (arr) => {
        const seen = new Set();
        return arr.filter(item => {
          const key = `${item?.name}-${item?.connection_id}-${item?.asset_name}`;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
      };

      impactsByFile[fileKey] = {
        direct: dedup(filteredDirectImpact),
        indirect: dedup(filteredIndirectImpact)
      };
    }

    // Calculate total impacts for collapsible section
    let totalImpacts = 0;
    for (const [_, impacts] of Object.entries(impactsByFile)) {
      totalImpacts += impacts.direct.length + impacts.indirect.length;
    }

    const shouldCollapse = totalImpacts > 20;
    
    if (shouldCollapse) {
      summary += `<details>\n<summary><b>Impact Analysis (${totalImpacts} items) - Click to expand</b></summary>\n\n`;
    }

    for (const [file, impacts] of Object.entries(impactsByFile)) {
      summary += `### ${file}\n`;
      
      if (impacts.direct.length > 0) {
        summary += `#### Directly Impacted (${impacts.direct.length})\n`;
        impacts.direct.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          summary += `- [${model?.name || 'Unknown'}](${url})\n`;
        });
      } else {
        summary += `#### No direct impacts found\n`;
      }
      
      if (impacts.indirect.length > 0) {
        summary += `\n#### Indirectly Impacted (${impacts.indirect.length})\n`;
        impacts.indirect.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          summary += `- [${model?.name || 'Unknown'}](${url})\n`;
        });
      } else {
        summary += `\n#### No indirect impacts found\n`;
      }
      
      summary += `\n`;
    }

    if (shouldCollapse) {
      summary += `</details>\n\n`;
    }

    // Process column changes
    const processColumnChanges = async (extension, extractor, isYml = false) => {
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

          const baseCols = safeArray(baseContent ? extractor(baseContent, file) : []);
          const headCols = safeArray(extractor(headContent, file));

          if (isYml) {
            const baseColNames = baseCols.map(col => col.name);
            const headColNames = headCols.map(col => col.name);

            const addedCols = headCols.filter(col => !baseColNames.includes(col.name));
            const removedCols = baseCols.filter(col => !headColNames.includes(col.name));

            added.push(...addedCols);
            removed.push(...removedCols);

            if (addedCols.length > 0 || removedCols.length > 0) {
              changes.push({ 
                file, 
                added: addedCols.map(c => c.name),
                removed: removedCols.map(c => c.name)
              });
            }
          } else {
            const addedCols = headCols.filter(col => !baseCols.includes(col));
            const removedCols = baseCols.filter(col => !headCols.includes(col));

            added.push(...addedCols);
            removed.push(...removedCols);

            if (addedCols.length > 0 || removedCols.length > 0) {
              changes.push({ file, added: addedCols, removed: removedCols });
            }
          }
        } catch (error) {
          core.error(`Error processing ${file}: ${error.message}`);
        }
      }

      return { changes, added, removed };
    };

    const { added: sqlAdded, removed: sqlRemoved } = await processColumnChanges(".sql", extractColumnsFromSQL);
    summary += `\n### SQL Column Changes\n`;
    summary += `Added columns(${sqlAdded.length}): ${sqlAdded.join(', ')}\n`;
    summary += `Removed columns(${sqlRemoved.length}): ${sqlRemoved.join(', ')}\n`;

    const { added: ymlAdded, removed: ymlRemoved } = await processColumnChanges(".yml", (content, file) => extractColumnsFromYML(content, file), true);
    summary += `\n### YML Column Changes\n`;
    summary += `Added columns(${ymlAdded.length}): ${ymlAdded.map(c => c.name).join(', ')}\n`;
    summary += `Removed columns(${ymlRemoved.length}): ${ymlRemoved.map(c => c.name).join(', ')}\n`;

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

    await core.summary
      .addRaw(summary)
      .write();
      
    core.setOutput("impact_markdown", summary);
  } catch (error) {
    core.setFailed(`[MAIN] Unhandled error: ${error.message}`);
    core.error(error.stack);
  }
};

run().catch(error => {
  core.setFailed(`[UNCAUGHT] Critical failure: ${error.message}`);
});