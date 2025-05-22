const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// Enhanced SQL parser with better error handling
const { 
  extractColumnsFromSQL = (content) => {
    try {
      if (!content) return [];
      // Basic SQL column extraction logic
      const columnMatches = content.match(/SELECT\s+(.*?)\s+FROM/is) || [];
      return columnMatches[1] ? columnMatches[1].split(',').map(col => col.trim()).filter(Boolean) : [];
    } catch (e) {
      core.error(`SQL parser error: ${e.message}`);
      return [];
    }
  },
  getFileContent = async (sha, filePath) => {
    try {
      if (!sha || !filePath) return null;
      // Implement your actual file content retrieval logic here
      return "SELECT column1, column2 FROM table"; // Mock implementation
    } catch (e) {
      core.error(`File content error: ${e.message}`);
      return null;
    }
  },
  extractColumnsFromYML = (content) => {
    try {
      if (!content) return [];
      // Basic YML column extraction logic
      const columnMatches = content.match(/columns:\s*\n([\s\S]*?)\n\s*-/i) || [];
      return columnMatches[1] ? columnMatches[1].split('\n').map(line => line.trim()).filter(line => line) : [];
    } catch (e) {
      core.error(`YML parser error: ${e.message}`);
      return [];
    }
  }
} = require("./sql-parser") || {};

// Configuration with validation
const config = {
  clientId: core.getInput("api_client_id") || "",
  clientSecret: core.getInput("api_client_secret") || "",
  changedFilesList: core.getInput("changed_files_list") || "",
  githubToken: core.getInput("GITHUB_TOKEN") || "",
  dqlabsBaseUrl: core.getInput("dqlabs_base_url") || "",
};

// Debug helper
const debug = (message, data = null) => {
  core.info(`[DEBUG] ${message}`);
  if (data) core.info(JSON.stringify(data, null, 2));
};

// Enhanced task matching
const findMatchingTasks = (tasks, changedModels) => {
  debug("Finding matching tasks", { changedModels });
  
  return tasks
    .filter(task => {
      const isDbt = task?.connection_type?.toLowerCase() === "dbt";
      const nameMatch = changedModels.some(model => 
        task?.name?.toLowerCase() === model.toLowerCase()
      );
      return isDbt && nameMatch;
    })
    .map(task => ({
      asset_id: task?.asset_id,
      connection_id: task?.connection_id,
      entity: task?.task_id,
      name: task?.name
    }))
    .filter(task => task.asset_id && task.connection_id && task.entity);
};

// Main analysis function
const analyzeImpact = async () => {
  try {
    debug("Starting impact analysis");
    
    // 1. Get changed files
    const changedFiles = config.changedFilesList
      .split(",")
      .map(f => f.trim())
      .filter(f => f.length > 0);
    
    debug("Changed files", changedFiles);

    // 2. Extract changed model names
    const changedModels = changedFiles
      .filter(file => file.endsWith(".sql"))
      .map(file => path.basename(file, ".sql"));
    
    debug("Changed models", changedModels);
    
    if (changedModels.length === 0) {
      core.info("No SQL models changed, skipping analysis");
      return { summary: "No SQL models changed", impacted: [] };
    }

    // 3. Get all tasks
    debug("Fetching tasks...");
    const tasksResponse = await axios.post(
      `${config.dqlabsBaseUrl}/api/pipeline/task/`,
      {
        chartType: 0,
        search: {},
        page: 0,
        pageLimit: 100,
        sortBy: "name",
        orderBy: "asc",
        date_filter: { days: "All", selected: "All" },
        chart_filter: {},
        is_chart: true,
      },
      {
        headers: {
          "Content-Type": "application/json",
          "client-id": config.clientId,
          "client-secret": config.clientSecret,
        }
      }
    );

    const allTasks = tasksResponse?.data?.response?.data || [];
    debug(`Found ${allTasks.length} total tasks`);

    // 4. Find matching tasks
    const matchedTasks = findMatchingTasks(allTasks, changedModels);
    debug(`Matched ${matchedTasks.length} tasks`, matchedTasks);

    if (matchedTasks.length === 0) {
      return { 
        summary: "No matching dbt tasks found for changed models",
        impacted: [] 
      };
    }

    // 5. Get lineage data
    const impacted = [];
    for (const task of matchedTasks) {
      debug(`Getting lineage for ${task.name}`);
      
      try {
        const lineageResponse = await axios.post(
          `${config.dqlabsBaseUrl}/api/lineage/entities/linked/`,
          {
            asset_id: task.asset_id,
            connection_id: task.connection_id,
            entity: task.entity
          },
          {
            headers: {
              "Content-Type": "application/json",
              "client-id": config.clientId,
              "client-secret": config.clientSecret,
            }
          }
        );

        const lineageData = lineageResponse?.data?.response?.data?.tables || [];
        debug(`Found ${lineageData.length} lineage items for ${task.name}`);

        impacted.push({
          model: task.name,
          direct: lineageData.filter(t => t.flow === "downstream"),
          indirect: [] // Will be populated later
        });
      } catch (error) {
        core.error(`Failed to get lineage for ${task.name}: ${error.message}`);
      }
    }

    // 6. Build summary
    let summary = "## Impact Analysis Results\n\n";
    summary += `**Changed Models:** ${changedModels.length}\n`;
    summary += `**Matched Tasks:** ${matchedTasks.length}\n\n`;

    let totalDirect = 0;
    let totalIndirect = 0;

    impacted.forEach(item => {
      summary += `### ${item.model}\n`;
      summary += `**Directly Impacts:** ${item.direct.length}\n`;
      item.direct.forEach(d => summary += `- ${d.name}\n`);
      
      // Add indirect impact analysis here if needed
      
      totalDirect += item.direct.length;
      totalIndirect += item.indirect.length;
    });

    summary += `\n**Total Direct Impacts:** ${totalDirect}\n`;
    summary += `**Total Indirect Impacts:** ${totalIndirect}\n`;

    return { summary, impacted };
  } catch (error) {
    core.error(`Analysis failed: ${error.message}`);
    throw error;
  }
};

// Main execution
const run = async () => {
  try {
    const { summary, impacted } = await analyzeImpact();
    
    // Post results
    core.info("\n" + summary);
    core.setOutput("impact_markdown", summary);

    if (github.context.payload.pull_request) {
      const octokit = github.getOctokit(config.githubToken);
      await octokit.rest.issues.createComment({
        owner: github.context.repo.owner,
        repo: github.context.repo.repo,
        issue_number: github.context.payload.pull_request.number,
        body: summary,
      });
    }

    await core.summary
      .addRaw(summary)
      .write();
  } catch (error) {
    core.setFailed(`Workflow failed: ${error.message}`);
  }
};

run();