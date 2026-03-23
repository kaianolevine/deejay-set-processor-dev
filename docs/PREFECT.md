# Prefect Cloud Setup

This project uses Prefect Cloud for flow observability.

## First time setup

1. Create a free account at app.prefect.cloud
2. Create a workspace
3. Generate an API key:
   Settings → API Keys → Create API Key
4. Add to GitHub Actions secrets:
   `PREFECT_API_KEY` = your api key
5. Add to GitHub Actions variables:
   `PREFECT_WORKSPACE` = your-account/your-workspace

## Local development

Set environment variables:

```bash
PREFECT_API_KEY=your-api-key
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}
```

Or use the CLI:

```bash
pip install prefect
prefect cloud login
```

## Flow runs

View all runs at: app.prefect.cloud

Each run shows: task-level logs, duration, success/failure
