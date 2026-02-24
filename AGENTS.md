# AGENTS.md

## Project Overview

This is an analytics repository that focuses on data analysis using ClickHouse database and Redash visualization platform. The repository contains query snippets for data analysis and automation scripts for working with the analytics platform.

## Repository Structure

- `redash_snippets.json` - Contains reusable SQL query snippets for ClickHouse
- `redash_queries.json` - Contains all SQL queries created by analysts in Redash (may contain errors but can be used as indirect data for enrichment)
- `redash_queries.csv` - CSV version of all SQL queries from Redash for easier analysis (may contain errors but can be used as indirect data for enrichment)
- `.env` - Environment variables for database connections and API tokens
- `requirements.txt` - Python dependencies
- `clickhouse_db/` - Directory containing DDL schemas for ClickHouse databases
- `clickhouse_export_ddl.sh` - Script to export ClickHouse database schemas
- `redash_export_snippets.py` - Python script to sync query snippets from Redash
- `redash_export_query.py` - Python script to extract all queries from Redash

## Key Components

### Data Sources
- **ClickHouse Database**: Primary analytics database ($CLICKHOUSE_DB) with multiple hosts
- **Redash Platform**: Visualization and dashboard platform at REDASH_HOST from .env file

### Query Snippets
The `redash_snippets.json` file contains reusable SQL fragments for common analytics tasks. 

### Analyst Queries
The `redash_queries.json` and `redash_queries.csv` files contain all SQL queries created by analysts in Redash:
- These queries represent actual analytical work performed by the team
- Queries may contain errors or inefficiencies as they reflect real-world usage
- They can be used as indirect data for enrichment and understanding common analytical patterns
- These files should be treated as historical records of analytical work rather than production-ready code

## Development Setup

1. Activate virtual environment:
   ```bash
   source .venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```


## Environment Variables

Required environment variables in `.env`:
- `DB_USER` - ClickHouse database user
- `DB_PASSWORD` - ClickHouse database password
- `CLICKHOUSE_DB` - Database name ($CLICKHOUSE_DB)
- `CLICKHOUSE_HOSTS` - Comma-separated list of ClickHouse hosts
- `REDASH_TOKEN` - API token for Redash platform
- `DASHBOARD_TEST_ID` - Test dashboard ID (optional)
- `DASHBOARD_TEST_SLUG` - Test dashboard slug (optional)

## Working with ClickHouse

Query examples:
```bash
# Simple SELECT with JSON response
curl -u "username:password" "http://host:8123/?database=default&query=SELECT+*+FROM+system.tables+LIMIT+3"

# Different response formats
curl -u "username:password" "http://host:8123/?query=SELECT+now()+AS+current_time&format=JSONEachRow"

# Using variables for better readability
HOST="your-clickhouse-server"
USER="username"
PASSWORD="password"
DATABASE="default"
QUERY="SELECT name, total_rows FROM system.tables WHERE database = currentDatabase()"

curl -s -u "$USER:$PASSWORD" \
     --get \
     --data-urlencode "query=$QUERY" \
     --data-urlencode "database=$DATABASE" \
     "http://$HOST:$PORT/?format=JSONCompact"
```

## Working with Redash API

- Getting queries, dashboards, and query snippets
- Creating new dashboards and queries
- Adding visualizations and widgets
- Pagination handling for large datasets

## Common Tasks

1. **Data Analysis**: Use query snippets from `redash_snippets.json` as building blocks
2. **Dashboard Creation**: Use Redash API to programmatically create dashboards
3. **Report Generation**: Export data to CSV files in `/reports/` directory
4. **Visualization**: Create HTML reports using ECharts library from CDN
5. **Historical Analysis**: Review analyst queries in `redash_queries.json` and `redash_queries.csv` to understand common analytical patterns and potentially reuse logic (note: these may contain errors and should be validated before use)


## Important Notes

- All variables must be loaded from `.env` file
- Follow KISS principles and use existing helpers
- Work with real DOM pages as specified by stakeholders
- Use proper character escaping in API requests
- Handle pagination when retrieving large datasets
- Store sensitive information only in environment variables

## Commands for Development

### Database Schema Management
```bash
# Export ClickHouse database schemas
./clickhouse_export_ddl.sh
```

### Query Snippets Management
```bash
# Update query snippets from Redash
python redash_export_snippets.py
```

### Query Extraction
```bash
# Extract all queries from Redash to JSON and CSV files
python redash_export_query.py
```


### Virtual Environment Management
```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Architecture Overview

### Data Flow
1. **Data Source**: ClickHouse database ($CLICKHOUSE_DB) containing analytics data
2. **Processing**: Python scripts and SQL queries process data
3. **Visualization**: Redash platform for dashboards and reports
4. **Storage**: Local storage of schemas and query snippets

### Key Directories
- `clickhouse_db/`: Contains database schema definitions organized by database name
- `reports/`: Output directory for generated reports and visualizations (if created)

### Integration Points
1. **ClickHouse API**: Direct database queries using curl or Python clients
2. **Redash API**: Programmatic dashboard and query management
3. **Environment Configuration**: All sensitive data managed through `.env` file

## Best Practices

1. **Performance**: Always use EXPLAIN to check query performance before execution
2. **Safety**: Queries processing more than 1 million rows should be reviewed
3. **Security**: Never hardcode credentials; always use environment variables
4. **Documentation**: Keep query snippets updated and well-documented
5. **Testing**: Validate all queries and scripts before production use
6. **Analyst Queries**: When using `redash_queries.json` and `redash_queries.csv` for data enrichment, remember these may contain errors and should be treated as historical records rather than production code

## Logic Analysis Framework

When evaluating hypotheses, data claims, or analytical conclusions, use this structured approach to assess validity and identify potential flaws in reasoning:

### Analysis Process

1. **Decompose the Statement**: Break down the main claim into a chain of logical steps or assumptions.
2. **Verify Data Support**: For each step, identify what data or facts confirm it. Check if supporting data is explicitly provided or what data would be needed for validation.
3. **Evaluate Causal Relationships**:
   *   Determine if the relationship between cause (A) and effect (B) is direct or influenced by hidden factors (C).
   *   Assess what indicates a true connection: temporal correlation, presence of control groups, exclusion of other factors.
   *   Consider reverse causality (B caused A) or influence of a common third factor.
4. **Identify Hidden Assumptions**: Recognize non-obvious assumptions underlying the claim (e.g., "users noticed the change", "other functionality had no impact", "external conditions remained stable").
5. **Consider Alternative Hypotheses**: Propose 2-3 other plausible explanations for the observed effect.
6. **Formulate Conclusions**: Assess how logical and substantiated the original claim is. Identify what is needed to prove or disprove the causal relationship (A/B testing, deeper data analysis, additional data collection).

### Application Guidelines

*   Apply this framework critically when reviewing analytical reports, metric changes, or proposed explanations for business phenomena.
*   Use it to structure your own analysis and ensure comprehensive evaluation of causal relationships.
*   Document your logic analysis process when presenting significant findings to stakeholders.
*   Remember that correlation does not imply causation; always seek evidence of direct causal mechanisms or controlled experimental validation.

### Detailed Logic Analysis Template

When analyzing specific hypotheses, data claims, or analytical conclusions, use this detailed template for thorough evaluation:

**Context:** [Briefly describe what this is about: launching a new feature, metric drop, campaign success, etc.]

**Hypothesis/Statement/Conclusion:** [Insert the text that needs verification. For example: "Conversion dropped 15% due to the 'Buy' button design update on October 15th."]

**Task:**
1.  **Decompose the Statement:** Break down the main statement into a chain of logical steps or assumptions.
2.  **Check the Data:** What data or facts confirm each step? Are they explicitly stated? If not, what data is needed for verification?
3.  **Evaluate Cause-and-Effect Relationships:**
    *   Is the relationship between cause (A) and effect (B) direct, or are there hidden factors (C)?
    *   What indicates a true connection: time correlation, presence of a control group, exclusion of other factors?
    *   Could there be reverse causality (B caused A) or influence of a common third factor?
4.  **Identify Hidden Assumptions:** What non-obvious assumptions underlie the statement? (For example: "users noticed the change", "other functionality didn't affect", "external conditions were stable").
5.  **Consider Alternative Hypotheses:** Propose 2-3 other plausible explanations for the observed effect.
6.  **Formulate a Conclusion:** How logical and substantiated is the original statement? What is needed to prove or disprove the cause-and-effect relationship (A/B test, deeper data analysis, additional data collection)?

**Answer Format:** Use a clear structure according to points 1-6. Be critical and constructive.

## Additional Analysis Prompts

### Black Swan Analysis Prompt (Non-obvious Connections)
For finding non-standard causes and hidden vulnerabilities in systems.

**Prompt:**

```
We observe a complex event or trend: [Describe the situation]. Help find indirect, non-obvious, or systemic cause-and-effect relationships that could have led to this.

**Use approaches:**
*   **Feedback loops:** Could an initial small change have triggered a reinforcing or balancing loop?
*   **Time delay (lag):** Could the cause have occurred significantly earlier than when the effect manifested?
*   **Intervention from another system:** Could a change in an seemingly unrelated process (e.g., logistics affecting conversion in an app) have caused this?
*   **Change in agent behavior:** How did users/employees/competitors change their behavior in response to our past actions, and how did this lead to the current result?

**Task:** Propose 2-3 "black swan" scenarios — plausible but non-obvious cause-and-effect chains explaining the current situation. For each scenario, indicate how it could have been predicted or tracked.
```

### Business Proposal Validation Prompt
To verify whether a proposed action will lead to the desired result.

**Prompt:**

```
Act as a "devil's advocate" for the proposed business solution. Check the "Action -> Result" chain for logical soundness.

**Goal:** [Desired outcome. For example: "Increase average check by 10%".]

**Proposed action:** [Specific step. For example: "Add recommendation for more expensive products to the cart".]

**Expected cause-and-effect relationship (authors' logic):** [Describe their assumptions. For example: "Users see recommendation -> evaluate its relevance -> add product to cart -> won't abandon purchase due to increased amount".]

**Task:**
1.  **Identify and challenge key assumptions** in the authors' logic. Which ones could be false?
2.  **Determine risks of side effects and anti-results** (e.g., increased cart abandonment due to interface overload).
3.  **Propose a counter-hypothesis:** Could this action lead to the opposite effect in the long-term?
4.  **What data would convince you** that the relationship between action and goal is truly cause-and-effect? What would be the ideal experiment to test this?
5.  **Formulate alternative (simpler/safer) actions** to achieve the same goal.

**Answer format:** List according to points 1-5. Focus on weak points in the logic.
```

### Retrospective Analysis Prompt (Root Cause Analysis)
Perfect for incident investigations or unexpected metric changes.

**Prompt:**

```
Conduct a retrospective analysis to identify the most probable causes of an event.

**Event (Effect):** [For example: "Sharp increase in churn among 'Premium' segment customers in November".]

**Known data and timeframes:** [List key facts, changes, dates. For example: "Service update on November 5, competitor promotion launch on November 1, seasonal decline last year".]

**Task:**
1.  **Build a timeline:** Compare the event with known changes (releases, marketing, external factors).
2.  **Formulate main cause hypotheses:** Based on data, propose 3-4 key hypotheses (technical, product, market, operational).
3.  **Apply "Occam's Razor" principle:** Which hypothesis requires the fewest new assumptions?
4.  **Evaluate verifiability:** For each hypothesis, determine what data or experiments could confirm/refute it.
5.  **Rank hypotheses** by probability and ease of verification.
6.  **Create a further analysis plan:** What needs to be analyzed first (logs, user surveys, cohort analysis)?

**Answer format:** Table with hypotheses, their rationale, verifiability, and priority. Then a brief action plan.
```
