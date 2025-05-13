const core = require('@actions/core');
const axios = require('axios');

(async () => {
  try {
    const clientId = core.getInput('api_client_id');
    const clientSecret = core.getInput('api_client_secret');
    const changedFiles = core.getInput('changed_files_list').split(',');

    // Step 1: Authenticate to DQLabs
    const authResp = await axios.post('https://your-dqlabs-api.com/oauth/token', {
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'client_credentials'
    });
    const accessToken = authResp.data.access_token;

    // Step 2: For each file, detect model name and call API (simplified here)
    const impactedAssets = [];

    for (const filePath of changedFiles) {
      const modelName = extractModelName(filePath); // You need to define this
      const lineageResp = await axios.get(`https://your-dqlabs-api.com/lineage/${modelName}`, {
        headers: { Authorization: `Bearer ${accessToken}` }
      });

      impactedAssets.push({
        model: modelName,
        downstream: lineageResp.data.downstream
      });
    }

    // Step 3: Format summary
    let summary = `## Impact Analysis Summary\n\n`;
    if (impactedAssets.length === 0) {
      summary += "No downstream impact detected.";
    } else {
      impactedAssets.forEach(item => {
        summary += `**${item.model}** impacts:\n`;
        item.downstream.forEach(d => {
          summary += `- ${d}\n`;
        });
        summary += '\n';
      });
    }

    // Step 4: Set output
    core.setOutput('impact_summary', summary);
  } catch (error) {
    core.setFailed(`Error: ${error.message}`);
  }
})();
