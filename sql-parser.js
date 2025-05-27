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

    // Case 1: Standard DBT format (models array)
    if (Array.isArray(schema.models)) {
      // Extract ALL columns from ALL models (if multiple exist)
      return schema.models.flatMap(model => 
        model.columns?.map(col => 
          typeof col === 'string' ? { name: col } : col
        ) || []
      );
    }

    // Case 2: Direct columns definition (fallback)
    if (schema.columns) {
      return schema.columns.map(col => 
        typeof col === 'string' ? { name: col } : col
      );
    }

    return []; // No columns found
  } catch (e) {
    console.error(`YML parsing error:`, e);
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