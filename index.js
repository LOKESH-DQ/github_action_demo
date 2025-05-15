const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const yaml = require("js-yaml");

const clientId = core.getInput("api_client_id");
const clientSecret = core.getInput("api_client_secret");
const changedFilesCSV = core.getInput("changed_files_list");

const token = core.getInput("GITHUB_TOKEN");
const octokit = github.getOctokit(token);
const context = github.context;

const fetchFileContent = async (path, ref) => {
  const { owner, repo } = context.repo;
  const response = await octokit.rest.repos.getContent({
    owner,
    repo,
    path,
    ref,
  });

  const content = Buffer.from(response.data.content, "base64").toString("utf-8");
  return content;
};

const getColumnDiffs = (oldCols = [], newCols = []) => {
  const oldMap = Object.fromEntries((oldCols || []).map((col) => [col.name, col]));
  const newMap = Object.fromEntries((newCols || []).map((col) => [col.name, col]));

  const added = newCols.filter((col) => !oldMap[col.name]);
  const deleted = oldCols.filter((col) => !newMap[col.name]);
  const updated = newCols.filter(
    (col) => oldMap[col.name] && JSON.stringify(oldMap[col.name]) !== JSON.stringify(col)
  );

  return { added, deleted, updated };
};

const getDownstreamAssets = async (asset_id, connection_id) => {
  const response = await axios.post(
    "http://44.238.88.190:8000/api/lineage/",
    {
      asset_id,
      connection_id,
      entity: asset_id,
    },
    {
      headers: {
        "client-id": clientId,
        "client-secret": clientSecret,
      },
    }
  );

  const all = response.data?.response?.data?.tables || [];
  return all.filter((x) => x.flow === "downstream");
};

const getJobAssets = async () => {
  const response = await axios.post(
    "http://44.238.88.190:8000/api/pipeline/job/",
    {},
    {
      headers: {
        "client-id": clientId,
        "client-secret": clientSecret,
      },
    }
  );

  return response.data?.response?.data || [];
};

const run = async () => {
  try {
    if (!context.payload.pull_request) {
      core.setFailed("This action only runs on pull_request events.");
      return;
    }

    if (!changedFilesCSV) {
      core.setFailed("No changed files provided.");
      return;
    }

    const changedFiles = changedFilesCSV
      .split(",")
      .map((f) => f.trim())
      .filter((f) => f.length > 0);
    if (changedFiles.length === 0) {
      core.setFailed("Changed files list is empty after processing.");
      return;
    }

    const baseRef = context.payload.pull_request?.base?.sha;
    const headRef = context.payload.pull_request?.head?.sha;

    if (!baseRef || !headRef) {
      core.setFailed("Cannot find base or head SHA from PR context.");
      return;
    }

    const jobAssets = await getJobAssets();

    let report = `🧠 **Impact Analysis Summary**\n\n`;
    report += `📄 **Changed DBT Models**:\n`;
    const modelDiffs = [];

    for (const filePath of changedFiles) {
      if (!filePath.endsWith(".yml")) continue;

      const [oldContent, newContent] = await Promise.all([
        fetchFileContent(filePath, baseRef).catch(() => null),
        fetchFileContent(filePath, headRef).catch(() => null),
      ]);

      if (!newContent) continue;

      let oldDoc = {};
      let newDoc = {};

      try {
        if (oldContent) oldDoc = yaml.load(oldContent) || {};
        newDoc = yaml.load(newContent) || {};
      } catch (err) {
        console.warn(`YAML parsing failed for ${filePath}: ${err.message}`);
        continue;
      }

      const oldModels = oldDoc.models || [];
      const newModels = newDoc.models || [];

      for (const newModel of newModels) {
        const oldModel = oldModels.find((m) => m.name === newModel.name);
        const diffs = getColumnDiffs(oldModel?.columns, newModel.columns);

        report += `- ${filePath}\n`;
        report += `  - 🏷️ Model: **${newModel.name}**\n`;
        report += `    - ➕ Added Columns: ${diffs.added.length}\n`;
        report += `    - 🛠️ Updated Columns: ${diffs.updated.length}\n`;
        report += `    - ❌ Deleted Columns: ${diffs.deleted.length}\n`;

        modelDiffs.push({
          name: newModel.name,
          added: diffs.added.length,
          updated: diffs.updated.length,
          deleted: diffs.deleted.length,
        });
      }
    }

    let downstream = [];

    for (const diff of modelDiffs) {
      const asset = jobAssets.find((a) => a.name === diff.name && a.connection_type === "dbt");

      if (asset) {
        console.log(
          `Fetching downstream for ${asset.name} (asset_id=${asset.asset_id}, connection_id=${asset.connection_id})`
        );
        const ds = await getDownstreamAssets(asset.asset_id, asset.connection_id);
        downstream = downstream.concat(ds);
      }
    }

    // Remove duplicates by name
    downstream = downstream.filter((v, i, a) => a.findIndex((x) => x.name === v.name) === i);

    if (downstream.length) {
      report += `\n🔗 **Downstream Assets**:\n`;
      downstream.forEach((d) => {
        report += `- ${d.name} (${d.connection_name})\n`;
      });
    } else {
      report += `\n🔗 **Downstream Assets**: None found\n`;
    }

    console.log(report);

    // Post comment on PR as you already do
    await octokit.rest.issues.createComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: context.payload.pull_request.number,
      body: report,
    });

    // Write the markdown summary for the GitHub Actions UI
    let summaryMd = `## 🧠 Impact Analysis Summary\n\n`;
    summaryMd += `### 📄 Changed DBT Models:\n`;
    modelDiffs.forEach((m) => {
      summaryMd += `- **${m.name}**\n`;
      summaryMd += `  - ➕ Added Columns: ${m.added}\n`;
      summaryMd += `  - 🛠️ Updated Columns: ${m.updated}\n`;
      summaryMd += `  - ❌ Deleted Columns: ${m.deleted}\n`;
    });
    summaryMd += `\n### 🔗 Downstream Assets:\n`;
    if (downstream.length > 0) {
      downstream.forEach((d) => {
        summaryMd += `- \`${d.name}\` (_${d.connection_name}_)\n`;
      });
    } else {
      summaryMd += `- None found\n`;
    }

    await core.summary
      .addRaw(summaryMd)
      .write();

    core.setOutput("downstream_assets", JSON.stringify(downstream));
  } catch (error) {
    core.setFailed(`Error: ${error.message}`);
  }
};

run();