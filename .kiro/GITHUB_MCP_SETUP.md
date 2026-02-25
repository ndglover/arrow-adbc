# GitHub MCP Server Setup Guide

This guide will help you set up the GitHub MCP server so Kiro can monitor and interact with your GitHub repository, including Actions workflows.

## 📋 Prerequisites

1. **Python & uv**: The GitHub MCP server runs via `uvx` (part of the `uv` Python package manager)
2. **GitHub Personal Access Token**: Required for authentication

## 🔧 Step 1: Install uv (Python Package Manager)

### Windows (PowerShell):
```powershell
# Using PowerShell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Alternative - Using pip:
```powershell
pip install uv
```

### Verify Installation:
```powershell
uv --version
uvx --version
```

## 🔑 Step 2: Create GitHub Personal Access Token

1. Go to: https://github.com/settings/tokens
2. Click **"Generate new token"** → **"Generate new token (classic)"**
3. Give it a descriptive name: `Kiro MCP Server`
4. Set expiration (recommend 90 days or No expiration for development)
5. Select the following scopes:
   - ✅ **repo** (Full control of private repositories)
     - Includes: repo:status, repo_deployment, public_repo, repo:invite, security_events
   - ✅ **workflow** (Update GitHub Action workflows)
   - ✅ **read:org** (Read org and team membership, read org projects)
   - ✅ **gist** (Create gists) - Optional but useful
6. Click **"Generate token"**
7. **IMPORTANT**: Copy the token immediately (you won't see it again!)

## 🔐 Step 3: Set Environment Variable

### Windows (PowerShell - Permanent):
```powershell
# Set for current user (persists across sessions)
[System.Environment]::SetEnvironmentVariable('GITHUB_TOKEN', 'your_token_here', 'User')

# Verify it's set
$env:GITHUB_TOKEN
```

### Windows (PowerShell - Current Session Only):
```powershell
$env:GITHUB_TOKEN = "your_token_here"
```

### Alternative - Add to Windows Environment Variables:
1. Press `Win + X` → **System**
2. Click **Advanced system settings**
3. Click **Environment Variables**
4. Under **User variables**, click **New**
5. Variable name: `GITHUB_TOKEN`
6. Variable value: `your_token_here`
7. Click **OK** and restart Kiro

## ✅ Step 4: Verify MCP Server

The MCP configuration is already set up in `.kiro/settings/mcp.json`. After setting the environment variable:

1. **Restart Kiro** (important for environment variable to be picked up)
2. Open the **MCP Server** view in Kiro's sidebar
3. You should see **"github"** server listed
4. Check if it shows as **Connected** (green indicator)

## 🎯 What You Can Do With GitHub MCP

Once configured, Kiro can:

### Repository Operations:
- 📂 Browse repository files and contents
- 🔍 Search across repositories
- 📝 Create/update files directly
- 🌿 Create branches
- 🔀 Create pull requests
- 🍴 Fork repositories

### GitHub Actions:
- 📊 **Monitor workflow runs** (check status of your CI/CD)
- 🔄 **View workflow logs** (see what failed in pre-commit checks)
- ▶️ **Trigger workflows** (run Custom Release workflow)
- 📈 **Check workflow status** (see if builds passed/failed)

### Issues & PRs:
- 🐛 Create and manage issues
- 💬 Comment on pull requests
- 🏷️ Add labels and milestones
- 👀 Review PR status and checks

## 🚀 Example Usage

After setup, you can ask Kiro:

```
"Check the status of the latest GitHub Actions run"
"Show me the logs from the failed pre-commit check"
"Trigger the Custom Release workflow with version suffix 'alpha1'"
"Create a pull request for this branch"
"What's the status of my CI/CD pipelines?"
```

## 🔧 Troubleshooting

### Server Not Connecting:
1. Verify `GITHUB_TOKEN` environment variable is set:
   ```powershell
   $env:GITHUB_TOKEN
   ```
2. Restart Kiro completely
3. Check MCP Server view for error messages

### Permission Errors:
- Ensure your token has the correct scopes (repo, workflow)
- Token might be expired - generate a new one

### uvx Not Found:
- Install uv: `pip install uv`
- Or use the PowerShell install script above
- Restart your terminal/Kiro after installation

## 📚 Additional Resources

- [GitHub MCP Server Documentation](https://github.com/modelcontextprotocol/servers/tree/main/src/github)
- [uv Installation Guide](https://docs.astral.sh/uv/getting-started/installation/)
- [GitHub Personal Access Tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)

## 🗑️ Cleanup (When Submitting to Apache)

When you're ready to submit your PR to Apache, you can:
1. Keep the MCP configuration (it's workspace-specific and won't affect Apache)
2. Or remove `.kiro/settings/mcp.json` if you prefer
3. Your `GITHUB_TOKEN` remains in your environment (not in the repo)
