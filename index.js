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
        ...(!isDirect && { depth: 10 }) // Add depth only for indirect impact
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

    // Match tasks with changed models
    const matchedTasks = tasks
      .filter(task => task?.connection_type === "dbt")
      .filter(task => changedModels.includes(task?.name))
      .map(task => ({
        ...task,
        entity: task?.task_id || "",
        filePath: changedFiles.find(f => path.basename(f, path.extname(f)) === task.name)
      }))
      .filter(task => task.filePath); // Ensure we have the file path

    // Store impacts per file
    const fileImpacts = {};

    // Initialize file impacts structure
    matchedTasks.forEach(task => {
      fileImpacts[task.filePath] = {
        direct: [],
        indirect: [],
        taskName: task.name
      };
    });

    // Process impact data for each file
    for (const task of matchedTasks) {
      // Get direct impacts (without depth)
      const directImpact = await getImpactAnalysisData(
        task.asset_id,
        task.connection_id,
        task.entity,
        true // isDirect = true
      );

      // Filter out the task itself from direct impacts
      const filteredDirectImpact = directImpact
        .filter(table => table?.name !== task.name)
        .filter(Boolean);

      fileImpacts[task.filePath].direct.push(...filteredDirectImpact);

      // Get indirect impacts (with depth=10)
      const indirectImpact = await getImpactAnalysisData(
        task.asset_id,
        task.connection_id,
        task.entity,
        false // isDirect = false
      );

      fileImpacts[task.filePath].indirect.push(...indirectImpact);
    }

    // Create unique key function for comparison
    const uniqueKey = (item) => `${item?.name}-${item?.connection_id}-${item?.asset_name}`;

    // Remove direct impacts from indirect results for each file
    Object.keys(fileImpacts).forEach(filePath => {
      const impacts = fileImpacts[filePath];
      const directKeys = new Set(impacts.direct.map(uniqueKey));
      impacts.indirect = impacts.indirect.filter(
        item => !directKeys.has(uniqueKey(item))
      );
    });

    // Deduplicate results within each file
    const dedup = (arr) => {
      const seen = new Set();
      return arr.filter(item => {
        const key = uniqueKey(item);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    };

    Object.keys(fileImpacts).forEach(filePath => {
      fileImpacts[filePath].direct = dedup(fileImpacts[filePath].direct);
      fileImpacts[filePath].indirect = dedup(fileImpacts[filePath].indirect);
    });

    const constructItemUrl = (item, baseUrl) => {
      if (!item || !baseUrl) return "#";

      try {
        const url = new URL(baseUrl);

        // Handle pipeline items
        if (item.asset_group === "pipeline") {
          if (item.is_transform) {
            url.pathname = `/observe/pipeline/transformation/${item.redirect_id}/run`;
          } else {
            url.pathname = `/observe/pipeline/task/${item.redirect_id}/run`;
          }
          return url.toString();
        }

        // Handle data items
        if (item.asset_group === "data") {
          url.pathname = `/observe/data/${item.redirect_id}/measures`;
          return url.toString();
        }

        // Default case
        return "#";
      } catch (error) {
        core.error(`Error constructing URL for ${item.name}: ${error.message}`);
        return "#";
      }
    };

    // Build the complete impacts section with single collapse
    const buildImpactsSection = (fileImpacts) => {
      let content = '';
      let totalDirect = 0;
      let totalIndirect = 0;
      
      // Generate content for each file
      Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
        const { direct, indirect, taskName } = impacts;
        totalDirect += direct.length;
        totalIndirect += indirect.length;

        content += `### File: ${filePath}\n`;
        content += `**Model:** ${taskName}\n\n`;
        
        content += `#### Directly Impacted (${direct.length})\n`;
        direct.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          content += `- [${model?.name || 'Unknown'}](${url})\n`;
        });

        content += `\n#### Indirectly Impacted (${indirect.length})\n`;
        indirect.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          content += `- [${model?.name || 'Unknown'}](${url})\n`;
        });

        content += '\n\n';
      });

      const totalImpacts = totalDirect + totalIndirect;
      const shouldCollapse = totalImpacts > 20;

      if (shouldCollapse) {
        return `<details>
<summary><b>Impact Analysis (${totalImpacts} total impacts - ${Object.keys(fileImpacts).length} files changed) - Click to expand</b></summary>

${content}
</details>`;
      }
      
      return content;
    };

    // Add impacts to summary
    summary += buildImpactsSection(fileImpacts);
    
    // Add summary of total impacts
    const totalDirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
    const totalIndirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
    
    summary += `\n## Summary of Impacts\n`;
    summary += `- **Total Directly Impacted:** ${totalDirect}\n`;
    summary += `- **Total Indirectly Impacted:** ${totalIndirect}\n`;
    summary += `- **Files Changed:** ${Object.keys(fileImpacts).length}\n\n`;

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

          // Handle YML columns differently
          if (isYml) {
            // Extract just the names for comparison
            const baseColNames = baseCols.map(col => col.name);
            const headColNames = headCols.map(col => col.name);

            const addedCols = headCols.filter(col => !baseColNames.includes(col.name));
            const removedCols = baseCols.filter(col => !headColNames.includes(col.name));

            // Get full column info for added/removed
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
            // Original SQL comparison logic
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

    // Process SQL changes
    const { added: sqlAdded, removed: sqlRemoved } = await processColumnChanges(".sql", extractColumnsFromSQL);
    summary += `\n### SQL Column Changes\n`;
    summary += `Added columns(${sqlAdded.length}): ${sqlAdded.join(', ')}\n`;
    summary += `Removed columns(${sqlRemoved.length}): ${sqlRemoved.join(', ')}\n`;

    // Process YML changes
    const { added: ymlAdded, removed: ymlRemoved } = await processColumnChanges(".yml", (content, file) => extractColumnsFromYML(content, file), true);
    summary += `\n### YML Column Changes\n`;
    summary += `Added columns(${ymlAdded.length}): ${ymlAdded.map(c => c.name).join(', ')}\n`;
    summary += `Removed columns(${ymlRemoved.length}): ${ymlRemoved.map(c => c.name).join(', ')}\n`;

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