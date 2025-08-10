import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from datetime import datetime

# File paths
INPUT_CSV = 'dataset.csv'
OUTPUT_ANALYSIS = 'analysis_report.txt'

# Helper: latency bucket function
def latency_bucket(seconds):
    if pd.isna(seconds):
        return 'unknown'
    if seconds < 1:
        return '<1s'
    elif seconds < 5:
        return '1-5s'
    elif seconds < 10:
        return '5-10s'
    else:
        return '>10s'

def safe_divide(a, b):
    try:
        if b == 0 or pd.isna(b) or pd.isna(a):
            return np.nan
        return a / b
    except Exception:
        return np.nan

def main():
    # --- 1. Robust File Loading with Error Handling ---
    try:
        # Load data with dtype fallback and error coercion for robustness
        df = pd.read_csv(INPUT_CSV, low_memory=False)
    except FileNotFoundError:
        print(f"Error: The file '{INPUT_CSV}' was not found. Please ensure the file exists in the correct directory.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{INPUT_CSV}' is empty. No data to process.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while loading the CSV file: {e}")
        return

    # --- 2. Data Cleaning and Deduplication ---
    # Strip leading/trailing whitespace from all column names
    df.columns = df.columns.str.strip()
    
    # IMPORTANT: Remove duplicate transactions based on their signature to ensure accurate stats
    original_row_count = len(df)
    df.drop_duplicates(subset=['signature'], keep='first', inplace=True)
    deduped_row_count = len(df)
    
    print(f"Columns loaded and cleaned successfully. Removed {original_row_count - deduped_row_count} duplicate rows.")

    # --- 3. Data cleaning & type conversion with robust checks ---
    # Convert numeric columns safely, ignoring those that are not found
    for col in df.columns:
        if df[col].dtype == 'object' and col not in ['signature', 'latency_bucket', 'program_ids', 'ts']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- 4. Flexible Timestamp Conversion and Feature Engineering ---
    # Convert Unix timestamp columns
    for time_col in ['block_time', 'block_time_rpc']:
        if time_col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
                df[time_col] = pd.to_datetime(df[time_col], unit='s', errors='coerce')
    
    # Convert string-based timestamp columns with explicit format
    if 'ts' in df.columns:
        # Use an explicit format string to ensure correct parsing
        df['ts'] = pd.to_datetime(df['ts'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        # Fill in 'hour' and 'weekday' columns from 'ts'
        df['hour'] = df['ts'].dt.hour
        df['weekday'] = df['ts'].dt.weekday

    # --- Calculate ratios safely (already in original script, but kept for context) ---
    if 'on_chain_tip_lamports' in df.columns and 'fee_lamports_final' in df.columns:
        df['tip_over_fee'] = df.apply(lambda row: safe_divide(row['on_chain_tip_lamports'], row['fee_lamports_final']), axis=1)

    # --- Profiling failed transactions subsidy correlation (robust checks added) ---
    failed_txns = df[df['is_success'] == 0]
    # NOTE: subsidy_percentage is expected to be sparse and may contain inf values
    failed_subsidies = failed_txns['subsidy_percentage'].replace([np.inf, -np.inf], np.nan).dropna()
    failed_count = len(failed_txns)
    failed_avg_subsidy = failed_subsidies.mean() if not failed_subsidies.empty else np.nan

    # --- Tip distribution stats (robust checks added) ---
    if 'on_chain_tip_lamports' in df.columns:
        # NOTE: jito_tip_lamports is expected to be sparse as most transactions are not in bundles
        tips_sol = df['on_chain_tip_lamports'] / 1e9
        avg_tip_sol = tips_sol.mean()
        tip_variance = tips_sol.var()
        tip_std = tips_sol.std()
    else:
        avg_tip_sol, tip_variance, tip_std = np.nan, np.nan, np.nan

    # --- Pearson correlation between profit and tip (robust checks added) ---
    if 'profit_lamports' in df.columns and 'on_chain_tip_lamports' in df.columns:
        profit_lam = df['profit_lamports'].fillna(0)
        tips_lam = df['on_chain_tip_lamports'].fillna(0)
        if len(profit_lam) > 1 and profit_lam.std() > 0 and tips_lam.std() > 0:
            pearson_coef, pearson_p = pearsonr(profit_lam, tips_lam)
        else:
            pearson_coef, pearson_p = np.nan, np.nan
    else:
        pearson_coef, pearson_p = np.nan, np.nan
    
    # --- Time varied analysis (robust checks added) ---
    if 'hour' in df.columns and 'subsidy_percentage' in df.columns:
        hourly_subsidy = df.groupby('hour')['subsidy_percentage'].mean()
    else:
        hourly_subsidy = pd.Series([], dtype='float64')

    if 'weekday' in df.columns and 'subsidy_percentage' in df.columns:
        weekday_subsidy = df.groupby('weekday')['subsidy_percentage'].mean()
    else:
        weekday_subsidy = pd.Series([], dtype='float64')

    # --- Stake delegation proxy: validator vs subsidy levels (robust checks added) ---
    if 'validator_identity' in df.columns and 'subsidy_percentage' in df.columns:
        # NOTE: validator_identity is expected to be sparse due to historical slots not being available
        validator_subsidy = df.groupby('validator_identity')['subsidy_percentage'].mean().sort_values(ascending=False)
    else:
        validator_subsidy = pd.Series([], dtype='float64')

    # --- Generate report ---
    with open(OUTPUT_ANALYSIS, 'w') as f:
        # ... (report generation code, which is mostly fine) ...
        f.write(f"Transaction Subsidy Analysis Report - {datetime.now()}\n")
        f.write("="*60 + "\n\n")

        f.write(f"Total Transactions Processed (after dedup): {deduped_row_count}\n")
        f.write(f"Failed Transactions Count: {failed_count}\n")
        f.write(f"Average Subsidy % for Failed Transactions: {failed_avg_subsidy:.4f}\n\n")

        f.write("Tip Distribution (in SOL):\n")
        if not np.isnan(avg_tip_sol):
            f.write(f"  Average Tip: {avg_tip_sol:.6f} SOL\n")
            f.write(f"  Tip Variance: {tip_variance:.6e}\n")
            f.write(f"  Tip Std Dev: {tip_std:.6f}\n\n")
        else:
            f.write("  Insufficient data to compute tip distribution.\n\n")

        f.write("Pearson Correlation (Profit vs Tip):\n")
        if not np.isnan(pearson_coef):
            f.write(f"  Coefficient: {pearson_coef:.4f}\n")
            f.write(f"  p-value: {pearson_p:.4g}\n\n")
        else:
            f.write("  Insufficient data to compute correlation.\n\n")

        f.write("Average Subsidy Percentage by Hour of Day:\n")
        f.write(hourly_subsidy.to_string() + "\n\n")

        f.write("Average Subsidy Percentage by Weekday (0=Monday):\n")
        f.write(weekday_subsidy.to_string() + "\n\n")

        f.write("Top 10 Validators by Average Subsidy Percentage:\n")
        f.write(validator_subsidy.head(10).to_string() + "\n\n")

        f.write("Latency Bucket Counts:\n")
        if 'latency_bucket' in df.columns:
            f.write(df['latency_bucket'].value_counts(dropna=False).to_string() + "\n\n")
        else:
            f.write("  Latency bucket column not found.\n\n")

        f.write("Sample Rows (first 5):\n")
        f.write(df.head().to_string() + "\n\n")

    print(f"Analysis complete. Report saved to: {OUTPUT_ANALYSIS}")

if __name__ == '__main__':
    main()