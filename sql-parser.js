const { execSync } = require('child_process');
const fs = require('fs');
const yaml = require('js-yaml');

// SQL Parser Functions
function extractColumnsFromSQL(content) {
  // Match columns in SELECT clause with various formatting
  const selectRegex = /SELECT\s+([\s\S]+?)\s+FROM/gi;
  const matches = selectRegex.exec(content);
  if (!matches) return [];

  const columnClause = matches[1]
    // Split columns while handling complex cases
    .split(/\s*,\s*(?![^(]*\))/) // Split on commas not inside parentheses
    .map(col => {
      // Extract column name/alias
      const cleaned = col
        .replace(/\s+as\s+.*/i, '') // Remove explicit aliases
        .replace(/.*\./g, '') // Remove table references
        .trim()
        .split(/\s+/)[0] // Get first token
        .replace(/[`"']/g, ''); // Remove quoting

      return cleaned.split('(')[0]; // Handle function calls
    })
    .filter(col => col && !col.startsWith('--')); // Filter comments

  return [...new Set(columnClause)]; // Remove duplicates
}

// Enhanced YML Parser Functions
function extractColumnsFromYML(content) {
  try {
    const schema = yaml.load(content);
    if (!schema) return [];
    
    const columns = [];
    
    // Handle different DBT YML structures
    if (Array.isArray(schema)) {
      // Standard array format (models/sources)
      schema.forEach(item => {
        if (item.columns) {
          // Direct columns definition
          item.columns.forEach(col => {
            if (typeof col === 'object') columns.push(col.name);
            else if (typeof col === 'string') columns.push(col);
          });
        } else if (item.models) {
          // Nested models definition
          item.models.forEach(model => {
            if (model.columns) {
              model.columns.forEach(col => columns.push(col.name));
            }
          });
        }
      });
    } else if (typeof schema === 'object') {
      // Single model definition or versioned schema
      if (schema.columns) {
        // Version 1 schema
        Object.values(schema.columns).forEach(col => {
          columns.push(col.name);
        });
      } else if (schema.models) {
        // Version 2 schema with models key
        schema.models.forEach(model => {
          if (model.columns) {
            Object.values(model.columns).forEach(col => {
              columns.push(col.name);
            });
          }
        });
      }
    }
    
    return [...new Set(columns)]; // Remove duplicates
  } catch (e) {
    console.error("YML parsing error:", e);
    return [];
  }
}

// Git Helper Function
function getFileContent(sha, filePath) {
  try {
    return execSync(`git show ${sha}:${filePath}`).toString();
  } catch (error) {
    return null; // File deleted in head
  }
}

module.exports = {
  extractColumnsFromSQL,
  extractColumnsFromYML,
  getFileContent
};