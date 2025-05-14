// const core = require('@actions/core');
// const axios = require('axios');

// (async () => {
//   try {
//     const clientId = core.getInput('api_client_id');
//     const clientSecret = core.getInput('api_client_secret');
//     const changedFiles = core.getInput('changed_files_list').split(',');

//     // Step 1: Authenticate to DQLabs
//     const authResp = await axios.post('http://44.238.88.190:8000/api/api_token', {
//       client_id: clientId,
//       client_secret: clientSecret,
//       grant_type: 'client_credentials'
//     });
//     const accessToken = authResp.data.access_token;

//     // Step 2: For each file, detect model name and call API (simplified here)
//     const impactedAssets = [];

//     for (const filePath of changedFiles) {
//       const modelName = extractModelName(filePath); // You need to define this logic
//       const lineageResp = await axios.get(`http://44.238.88.190:8000/api/lineage/${modelName}`, {
//         headers: { Authorization: `Bearer ${accessToken}` }
//       });

//       impactedAssets.push({
//         model: modelName,
//         downstream: lineageResp.data.downstream // Ensure this is the correct API response
//       });
//     }

//     // Step 3: Format summary
//     let summary = `## Impact Analysis Summary\n\n`;
//     if (impactedAssets.length === 0) {
//       summary += "No downstream impact detected.";
//     } else {
//       impactedAssets.forEach(item => {
//         summary += `**${item.model}** impacts:\n`;
//         item.downstream.forEach(d => {
//           summary += `- ${d}\n`;
//         });
//         summary += '\n';
//       });
//     }

//     // Step 4: Set output
//     core.setOutput('impact_summary', summary);
//   } catch (error) {
//     core.setFailed(`Error: ${error.message}`);
//   }
// })();

// // Helper function to extract model name from file path
// function extractModelName(filePath) {
//   // Adjust the logic based on your naming conventions
//   // For example, strip off extensions and directories
//   const modelName = filePath.split('/').pop().replace(/\.(sql|yml)$/, '');
//   return modelName;
// }


#!/usr/bin/env node
'use strict';

const core = require('@actions/core');
const github = require('@actions/github');
const axios = require('axios');

async function run() {
  try {
    const clientId        = core.getInput('api_client_id', { required: true });
    const clientSecret    = core.getInput('api_client_secret', { required: true });
    const changedFilesCSV = core.getInput('changed_files_list', { required: true });
    const changedFiles    = changedFilesCSV.split(',').map(f => f.trim()).filter(Boolean);

    core.info(`Client ID: ${clientId}`);
    core.info(`Client Secret: ${clientSecret}`);

    const token = await authenticate(clientId, clientSecret);

    let reportLines = [];
    let totalImpacted = 0;

    for (const path of changedFiles) {
      if (!path.match(/\.(sql|yml)$/)) {
        core.info(`Skipping non-DBT file: ${path}`);
        continue;
      }

      const status = await getFileStatus(path); // TODO
      const modelName = extractModelName(path);

      core.info(`→ ${path} [${status}] → model=${modelName}`);

      if (status === 'added') {
        reportLines.push(`- **${modelName}** (added): no impact analysis needed.`);
        continue;
      }

      if (status === 'deleted') {
        const downstream = await getDownstreamModels(token, modelName);
        reportLines.push(formatImpact(`Deleted model ${modelName}`, downstream));
        totalImpacted += downstream.length;
        continue;
      }

      const diff = await getColumnDiff(path);
      if (diff.added.length || diff.removed.length) {
        const assets = await getColumnLineage(token, modelName, [...diff.added, ...diff.removed]);
        reportLines.push(formatColumnImpact(modelName, diff, assets));
        totalImpacted += assets.length;
      } else if (diff.modified.length) {
        const assets = await getColumnUsage(token, modelName, diff.modified);
        reportLines.push(formatColumnUsage(modelName, diff, assets));
        totalImpacted += assets.length;
      } else {
        reportLines.push(`- **${modelName}** (modified): no column-level changes detected.`);
      }
    }

    const finalReport = reportLines.join('\n');
    core.setOutput('impact_summary', finalReport);

    if (process.env.GITHUB_TOKEN) {
      await postPrComment(finalReport);
    }
  } catch (err) {
    core.setFailed(err.message);
  }
}

async function authenticate(id, secret) {
  const resp = await axios.post(
    'http://44.238.88.190:8000/api/api_token',
    { grant_type: 'client_credentials', client_id: id, client_secret: secret }
  );
  return resp.data.access_token;
}

async function getFileStatus(path) {
  return 'modified'; // Stub
}

function extractModelName(path) {
  return path.replace(/^.*?/models\//, '').replace(/\.(sql|yml)$/, '').split('/').join('.');
}

async function getDownstreamModels(token, model) {
  const resp = await axios.get(
    `http://44.238.88.190:8000/api/lineage/downstream?model=${encodeURIComponent(model)}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  return resp.data.impacted_models || [];
}

async function getColumnLineage(token, model, cols) {
  const resp = await axios.post(
    'http://44.238.88.190:8000/api/lineage/column',
    { model, columns: cols },
    { headers: { Authorization: `Bearer ${token}` } }
  );
  return resp.data.impacted_assets || [];
}

async function getColumnUsage(token, model, cols) {
  const resp = await axios.post(
    'http://44.238.88.190:8000/api/lineage/usage',
    { model, columns: cols },
    { headers: { Authorization: `Bearer ${token}` } }
  );
  return resp.data.impacted_assets || [];
}

function formatImpact(title, items) {
  if (!items.length) return `- **${title}**: no downstream impacts.`;
  if (items.length <= 20) {
    return `- **${title}** impacts:\n${items.map(i => `  - ${i}`).join('\n')}`;
  }
  return [
    `- **${title}** impacts (${items.length}):`,
    `<details><summary>Show impacted models</summary>`,
    items.map(i => `  - ${i}`).join('\n'),
    `</details>`
  ].join('\n');
}

function formatColumnImpact(model, diff, items) {
  const changes = [
    diff.added.length && `added: ${diff.added.join(', ')}`,
    diff.removed.length && `removed: ${diff.removed.join(', ')}`
  ].filter(Boolean).join('; ');
  const header = `- **${model}** columns [${changes}]`;
  if (!items.length) return `${header}: no impacted assets.`;
  return formatImpact(header, items);
}

function formatColumnUsage(model, diff, items) {
  const cols = diff.modified.join(', ');
  const header = `- **${model}** modified columns [${cols}] used by downstream`;
  if (!items.length) return `${header}: no impacted assets.`;
  return formatImpact(header, items);
}

async function getColumnDiff(path) {
  return { added: [], removed: [], modified: [] }; // Stub
}

async function postPrComment(body) {
  const token = process.env.GITHUB_TOKEN;
  const octo = github.getOctokit(token);
  const { repo, issue } = github.context.issue;
  await octo.rest.issues.createComment({
    ...repo,
    issue_number: issue.number,
    body: `## :mag: DBT Impact Analysis Report\n\n${body}`
  });
}

run();