const { execSync } = require('child_process');
const fs = require('fs');
const yaml = require('js-yaml');
const path = require('path');

// SQL Parser (unchanged)
function extractColumnsFromSQL(content) {
  const selectRegex = /SELECT\s+([\s\S]+?)\s+FROM/gi;
  const matches = selectRegex.exec(content);
  if (!matches) return [];

  return matches[1]
    .split(/\s*,\s*(?![^(]*\))/)
    .map(col => {
      const cleaned = col
        .replace(/\s+as\s+.*/i, '')
        .replace(/.*\./g, '')
        .trim()
        .split(/\s+/)[0]
        .replace(/[`"']/g, '');
      return cleaned.split('(')[0];
    })
    .filter(col => col && !col.startsWith('--'));
}

// Enhanced YML Parser with specific support for your format
function extractColumnsFromYML(content, filePath) {
  try {
    const schema = yaml.load(content);
    if (!schema) return [];

    const modelName = path.basename(filePath, '.yml');

    // Case 1: Custom format with model.attributes
    if (schema.model?.name === modelName && schema.model.attributes) {
      return Object.entries(schema.model.attributes).map(([name, def]) => ({
        name,
        ...(typeof def === 'object' ? def : {})
      }));
    }

    // Case 2: Custom format with model.columns
    if (schema.model?.name === modelName && schema.model.columns) {
      return schema.model.columns.map(col =>
        typeof col === 'string' ? { name: col } : col
      );
    }

    // Case 3: Standard DBT format with models: [...]
    if (Array.isArray(schema.models)) {
      const model = schema.models.find(m => m.name === modelName);
      return model?.columns?.map(col =>
        typeof col === 'string' ? { name: col } : col
      ) || [];
    }

    // Case 4: Older DBT format: top-level array
    if (Array.isArray(schema)) {
      const model = schema.find(m => m.name === modelName);
      return model?.columns?.map(col =>
        typeof col === 'string' ? { name: col } : col
      ) || [];
    }

    // Case 5: Flat structure
    if (schema.columns) {
      return schema.columns.map(col =>
        typeof col === 'string' ? { name: col } : col
      );
    }

    return [];
  } catch (e) {
    console.error(`YML parsing error (${path.basename(filePath)}):`, e.message);
    return [];
  }
}

// Enhanced Git Helper with better error handling
function getFileContent(sha, filePath) {
  try {
    return execSync(`git show ${sha}:${filePath}`, { 
      stdio: ['pipe', 'pipe', 'ignore'],
      encoding: 'utf-8',
      maxBuffer: 10 * 1024 * 1024 // 10MB buffer for large files
    });
  } catch (error) {
    if (error.message.includes('exists on disk, but not in')) {
      console.log(`File not found in ${sha}: ${filePath}`);
    } else {
      console.error(`Error reading ${filePath}:`, error.message);
    }
    return null;
  }
}

module.exports = {
  extractColumnsFromSQL,
  extractColumnsFromYML,
  getFileContent
};