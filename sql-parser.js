const { execSync } = require('child_process');
const fs = require('fs');
 
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
 
function getFileContent(sha, filePath) {
  try {
    return execSync(`git show ${sha}:${filePath}`).toString();
  } catch (error) {
    return null; // File deleted in head
  }
}
 
module.exports = {
  extractColumnsFromSQL,
  getFileContent
};