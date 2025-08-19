# CRM Pipeline Analytics

A simple Python tool for analyzing CRM lead data and generating weekly reports with forecasts.

## What it does

- Loads CRM data from CSV files
- Calculates weekly metrics (new leads, MQL, SQL, wins)
- Shows conversion rates at each stage
- Breaks down performance by channel and region
- Creates a 4-week forecast using moving averages

## Requirements

```
pandas
numpy
matplotlib
```

## Setup

1. Clone this repo
2. Install requirements: `pip install pandas numpy matplotlib`
3. Put your CRM data in `crm_leads.csv`

## Usage

### With your own data
Make sure your CSV has these columns:
- lead_id
- created_at (date when lead was created)
- mql_at (date when became MQL, empty if not yet)
- sql_at (date when became SQL, empty if not yet)  
- won_at (date when won, empty if not yet)
- channel (Ads, Organic, etc)
- region (optional)

Then run:
```bash
python crm_pipeline_analytics_final.py
```

## Output Files

The script creates an `output/` folder with:
- `weekly_summary.csv` - Weekly KPIs and rates
- `channel_breakdown.csv` - Performance by channel
- `region_breakdown.csv` - Performance by region
- `forecast.csv` - 4-week forecast

## Sample Output

```
Pipeline completed. 800 records processed.
Generated 16 weeks of data.
Results saved to output/
```

## File Structure

```
crm_pipeline_analytics_final.py  # Main script
crm_leads.csv                    # Your CRM data (or generated)
output/                          # Results folder
├── weekly_summary.csv
├── channel_breakdown.csv  
├── region_breakdown.csv
└── forecast.csv
```

## Notes

- Uses Monday as start of week
- Forecast is based on 4-week moving average
- Handles missing dates gracefully
- All dates should be in ISO format (YYYY-MM-DD or full timestamp)

Built for analyzing lead pipeline performance and creating weekly reports for stakeholders.
