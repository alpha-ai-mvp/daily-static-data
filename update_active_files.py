#!/usr/bin/env python3
"""
Update Active Static Data Files
===============================

This script updates the static data files that are actively used in the Alpha Analytics charts:
- volatility.csv (Used in EM-04 - Equity Volatility Indices)
- general_rates_db_CreditSpreads.csv (Used in EM-06 - Credit-Spread Trendline)
- general_rates_db_InterestRates.csv (Used in EM-01 and EM-03 - Treasury Yield Curve and Historical Rates)

FEATURES:
- Retry logic with exponential backoff for network failures
- Better error handling and reporting
- Success rate tracking
- Detailed logging
- Incremental updates (only fetch last 2 weeks and append)
"""

import os
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
import logging
import time
import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === CONFIGURATION ===
STATIC_GENERAL_PATH = "StaticGeneral.csv"
STATIC_FRED_PATH = "StaticFred.csv"
OUTPUT_DIR = "."  # Current directory (Alpha AI MVP static data)
FRED_API_KEY = os.getenv("FRED_API_KEY")
LOOKBACK_YEARS = 50  # 50 years of historical data (for full refresh)
INCREMENTAL_DAYS = 14  # Fetch and replace last 2 weeks + add new data for incremental updates

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
RETRY_BACKOFF = 2  # exponential backoff multiplier

# Target tables/files to update
TARGET_TABLES = [
    "general_rates_db.InterestRates",
    "general_rates_db.CreditSpreads",
    "general_borrowing_db.MortgageRates"
]

# === UTILITY FUNCTIONS ===
def get_last_date_from_csv(csv_path):
    """Get the last date from an existing CSV file"""
    try:
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            if 'date' in df.columns and not df.empty:
                # Convert to datetime and get the max date
                df['date'] = pd.to_datetime(df['date'])
                last_date = df['date'].max()
                logger.info(f"📅 Last date in {csv_path}: {last_date.strftime('%Y-%m-%d')}")
                return last_date
        return None
    except Exception as e:
        logger.warning(f"⚠️ Could not read last date from {csv_path}: {e}")
        return None

def determine_fetch_period(csv_path, incremental=True):
    """Determine the date range to fetch data for"""
    if incremental:
        last_date = get_last_date_from_csv(csv_path)
        if last_date:
            # Start from 2 weeks before the last date to replace recent data
            start_date = (last_date - timedelta(days=INCREMENTAL_DAYS)).strftime('%Y-%m-%d')
            end_date = datetime.today().strftime('%Y-%m-%d')
            logger.info(f"📊 Incremental update: fetching from {start_date} to {end_date}")
            logger.info(f"📊 This will replace the last {INCREMENTAL_DAYS} days and add new data")
        else:
            # If no existing file, fetch last 2 weeks
            start_date = (datetime.today() - timedelta(days=INCREMENTAL_DAYS)).strftime('%Y-%m-%d')
            end_date = datetime.today().strftime('%Y-%m-%d')
            logger.info(f"📊 No existing file: fetching last {INCREMENTAL_DAYS} days ({start_date} to {end_date})")
    else:
        # Full refresh - fetch all historical data
        start_date = (datetime.today() - timedelta(days=LOOKBACK_YEARS * 365)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
        logger.info(f"📊 Full refresh: fetching from {start_date} to {end_date}")
    
    return start_date, end_date

def merge_and_save_data(new_data, csv_path, incremental=True):
    """Merge new data with existing CSV file and save"""
    try:
        if incremental and os.path.exists(csv_path):
            # Read existing data
            existing_data = pd.read_csv(csv_path)
            existing_data['date'] = pd.to_datetime(existing_data['date'])
            
            # Prepare new data
            new_data['date'] = pd.to_datetime(new_data['date'])
            
            # Find overlapping dates (data being replaced)
            overlapping_dates = existing_data[existing_data['date'].isin(new_data['date'])]
            new_dates = new_data[~new_data['date'].isin(existing_data['date'])]
            
            # Combine data
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            
            # Remove duplicates (keep the latest version - the new data)
            combined_data = combined_data.drop_duplicates(subset=['date'], keep='last')
            
            # Sort by date
            combined_data = combined_data.sort_values('date')
            
            # Convert date back to string for CSV
            combined_data['date'] = combined_data['date'].dt.strftime('%Y-%m-%d')
            
            logger.info(f"📊 Processing {len(new_data)} fetched records:")
            logger.info(f"  🔄 Replaced {len(overlapping_dates)} existing records")
            logger.info(f"  ➕ Added {len(new_dates)} new records")
            logger.info(f"📊 Total records after merge: {len(combined_data)}")
            
            # Save merged data
            combined_data.to_csv(csv_path, index=False)
            return len(combined_data)
        else:
            # No existing file or full refresh - just save new data
            new_data['date'] = pd.to_datetime(new_data['date']).dt.strftime('%Y-%m-%d')
            new_data.to_csv(csv_path, index=False)
            logger.info(f"📊 Saved {len(new_data)} records to {csv_path}")
            return len(new_data)
            
    except Exception as e:
        logger.error(f"❌ Error merging and saving data to {csv_path}: {e}")
        return 0

def fetch_fred_series_with_retry(fred_client, fred_id, primary_key, start_date, end_date=None, max_retries=MAX_RETRIES):
    """Fetch a single FRED series with retry logic"""
    for attempt in range(max_retries):
        try:
            attempt_msg = f" - Attempt {attempt + 1}/{max_retries}" if attempt > 0 else ""
            logger.info(f"  • Fetching {fred_id} ({primary_key}){attempt_msg}")
            
            # Add end_date parameter if provided
            if end_date:
                data = fred_client.get_series(fred_id, observation_start=start_date, observation_end=end_date)
            else:
                data = fred_client.get_series(fred_id, observation_start=start_date)
            
            if data is not None and not data.empty:
                logger.info(f"    ✅ Success: {len(data)} records")
                return data
            else:
                logger.warning(f"    ⚠️ No data returned for {fred_id}")
                return None
                
        except Exception as e:
            if attempt < max_retries - 1:
                delay = RETRY_DELAY * (RETRY_BACKOFF ** attempt)
                logger.warning(f"    ⚠️ Attempt {attempt + 1} failed for {fred_id}: {str(e)[:100]}...")
                logger.info(f"    🔄 Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"    ❌ All {max_retries} attempts failed for {fred_id} ({primary_key})")
                return None
    return None

def split_static_general(df):
    """Split StaticGeneral.csv into FRED and Yahoo Finance data sources"""
    fred_df = df[df['fred_id'].notnull()][['primary_key', 'fred_id']].drop_duplicates()
    yahoo_df = df[df['yahoo_id'].notnull()][['primary_key', 'yahoo_id']].drop_duplicates()
    return fred_df, yahoo_df

def fetch_fred_data(api_key, fred_df, start_date, end_date=None):
    """Fetch data from FRED API with retry logic"""
    logger.info("📊 Fetching FRED data...")
    fred = Fred(api_key=api_key)
    df = pd.DataFrame()
    
    successful_fetches = 0
    failed_fetches = 0
    
    for idx, row in fred_df.iterrows():
        primary_key = row['primary_key']
        fred_id = row['fred_id']
        
        data = fetch_fred_series_with_retry(fred, fred_id, primary_key, start_date, end_date)
        
        if data is not None:
            data.name = primary_key
            df = pd.concat([df, data], axis=1) if not df.empty else pd.DataFrame(data)
            successful_fetches += 1
        else:
            failed_fetches += 1
    
    if not df.empty:
        df.dropna(how='all', inplace=True)
        df.reset_index(inplace=True)
        df.rename(columns={df.columns[0]: "date"}, inplace=True)
        logger.info(f"✅ FRED data fetched: {len(df)} rows, {len(df.columns)-1} series")
        if successful_fetches + failed_fetches > 0:
            success_rate = successful_fetches / (successful_fetches + failed_fetches) * 100
            logger.info(f"📊 Success rate: {successful_fetches}/{successful_fetches + failed_fetches} series ({success_rate:.1f}%) ")
    else:
        logger.error("❌ No FRED data was successfully fetched")
    
    return df

def export_table_csv(table_name, df, general_df, output_dir, incremental=True):
    """Export specific table to CSV file with incremental update support"""
    relevant = general_df[general_df['database.table_insert'] == table_name]
    required_pks = relevant['primary_key'].tolist()

    # Check if all required keys are in the DataFrame
    missing_keys = [pk for pk in required_pks if pk not in df.columns]
    if missing_keys:
        logger.warning(f"⏭️ Skipping {table_name}: missing keys -> {missing_keys}")
        logger.info(f"    💡 Available keys: {[col for col in df.columns if col != 'date']}")
        return False

    columns = ['date'] + required_pks
    data = df[columns].dropna(subset=required_pks, how='all')

    if not data.empty:
        output_path = os.path.join(output_dir, f"{table_name.replace('.', '_')}.csv")
        
        # Use the new merge and save function
        total_records = merge_and_save_data(data, output_path, incremental)
        
        if total_records > 0:
            logger.info(f"✅ Updated: {output_path} (Total: {total_records} rows, {len(required_pks)} series)")
            return True
        else:
            logger.error(f"❌ Failed to update {output_path}")
            return False
    else:
        logger.warning(f"⚠️ No non-empty data for {table_name}. Skipped.")
        return False

def update_volatility_data(incremental=True):
    """Update volatility.csv using StaticFred.csv with incremental support"""
    logger.info("📈 Updating volatility data...")
    
    try:
        # Read StaticFred.csv to get volatility series
        static_fred = pd.read_csv(STATIC_FRED_PATH)
        volatility_series = static_fred[static_fred['category'] == 'volatility']
        
        if volatility_series.empty:
            logger.error("❌ No volatility series found in StaticFred.csv")
            return False
        
        # Determine date range
        volatility_path = os.path.join(OUTPUT_DIR, "volatility.csv")
        start_date, end_date = determine_fetch_period(volatility_path, incremental)
        
        # Fetch volatility data from FRED
        fred = Fred(api_key=FRED_API_KEY)
        
        df = pd.DataFrame()
        successful_fetches = 0
        failed_fetches = 0
        
        for idx, row in volatility_series.iterrows():
            name = row['name_pk']
            fred_id = row['fred_id']
            
            data = fetch_fred_series_with_retry(fred, fred_id, name, start_date, end_date)
            
            if data is not None:
                data.name = name
                df = pd.concat([df, data], axis=1) if not df.empty else pd.DataFrame(data)
                successful_fetches += 1
            else:
                failed_fetches += 1
        
        if not df.empty:
            df.dropna(how='all', inplace=True)
            df.reset_index(inplace=True)
            df.rename(columns={df.columns[0]: "date"}, inplace=True)
            
            # Use merge and save function
            total_records = merge_and_save_data(df, volatility_path, incremental)
            
            if total_records > 0:
                logger.info(f"✅ Updated: {volatility_path} (Total: {total_records} rows, {len(df.columns)-1} series)")
                if successful_fetches + failed_fetches > 0:
                    success_rate = successful_fetches / (successful_fetches + failed_fetches) * 100
                    logger.info(f"📊 Success rate: {successful_fetches}/{successful_fetches + failed_fetches} series ({success_rate:.1f}%)")
                return True
            else:
                logger.error("❌ Failed to update volatility data")
                return False
        else:
            logger.error("❌ No volatility data was successfully fetched")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error updating volatility data: {e}")
        return False

def update_rates_and_credit_data(incremental=True):
    """Update interest rates and credit spreads data using StaticGeneral.csv with incremental support"""
    logger.info("📊 Updating rates and credit spreads data...")
    
    try:
        # Read StaticGeneral.csv
        if not os.path.exists(STATIC_GENERAL_PATH):
            logger.error(f"❌ {STATIC_GENERAL_PATH} not found")
            return False
        
        static_general = pd.read_csv(STATIC_GENERAL_PATH)
        
        # Filter for target tables only
        target_data = static_general[static_general['database.table_insert'].isin(TARGET_TABLES)]
        
        if target_data.empty:
            logger.error("❌ No data found for target tables")
            return False
        
        # Split into FRED and Yahoo Finance data
        fred_df, yahoo_df = split_static_general(target_data)
        
        if fred_df.empty:
            logger.error("❌ No FRED data found for target tables")
            return False
        
        # Determine date range based on existing files
        # Use the first target table to determine the date range
        first_table_path = os.path.join(OUTPUT_DIR, f"{TARGET_TABLES[0].replace('.', '_')}.csv")
        start_date, end_date = determine_fetch_period(first_table_path, incremental)
        
        # Fetch FRED data
        fred_data = fetch_fred_data(FRED_API_KEY, fred_df, start_date, end_date)
        
        if fred_data.empty:
            logger.error("❌ No FRED data was successfully fetched")
            return False
        
        # Export each target table
        success_count = 0
        for table_name in TARGET_TABLES:
            logger.info(f"📝 Updating table: {table_name}")
            if export_table_csv(table_name, fred_data, static_general, OUTPUT_DIR, incremental):
                success_count += 1
        
        logger.info(f"✅ Successfully updated {success_count}/{len(TARGET_TABLES)} tables")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"❌ Error updating rates and credit data: {e}")
        return False

def update_sentiment_data():
    """Downloads the latest sentiment data from AAII."""
    logger.info("📈 Updating sentiment data...")
    url = "https://www.aaii.com/files/surveys/sentiment.xls"
    output_path = os.path.join(OUTPUT_DIR, "sentiment.xls")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.aaii.com/sentiment-survey', # Assuming this is the page where the download link is
        'DNT': '1', # Do Not Track Request Header
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        with open(output_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"✅ Successfully downloaded and saved sentiment data to {output_path}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error downloading sentiment data: {e}")
        return False

def main(incremental=True):
    """Main function to update all active data files"""
    mode = "INCREMENTAL" if incremental else "FULL REFRESH"
    logger.info(f"🚀 Starting {mode} update of active static data files...")
    logger.info(f"📁 Output directory: {os.path.abspath(OUTPUT_DIR)}")
    logger.info(f"🔄 Retry configuration: {MAX_RETRIES} attempts with {RETRY_DELAY}s base delay (exponential backoff)")

    if not FRED_API_KEY:
        logger.error("❌ Missing FRED_API_KEY environment variable.")
        logger.error("💡 Set FRED_API_KEY before running this script.")
        return False

    if incremental:
        logger.info(f"📅 Incremental mode: replacing last {INCREMENTAL_DAYS} days and adding new data")
    else:
        logger.info(f"📅 Full refresh mode: fetching {LOOKBACK_YEARS} years of data and overwriting files")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Track success/failure
    results = {}

    # 1. Update volatility data
    logger.info("============================================================")
    logger.info("1. UPDATING VOLATILITY DATA")
    logger.info("============================================================")
    results['volatility'] = update_volatility_data(incremental)

    # 2. Update rates and credit spreads data
    logger.info("============================================================")
    logger.info("2. UPDATING RATES AND CREDIT SPREADS DATA")
    logger.info("============================================================")
    results['rates_credit'] = update_rates_and_credit_data(incremental)

    # 3. Update sentiment data
    logger.info("============================================================")
    logger.info("3. UPDATING SENTIMENT DATA")
    logger.info("============================================================")
    results['sentiment'] = update_sentiment_data()

    # Summary
    logger.info("============================================================")
    logger.info("📊 UPDATE SUMMARY")
    logger.info("============================================================")

    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)

    for task, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"{task.upper()}: {status}")

    logger.info(f"🎉 Overall: {success_count}/{total_count} tasks completed successfully")

    if success_count == total_count:
        logger.info(f"🎯 All active data files have been updated successfully! ({mode})")
    else:
        logger.warning("⚠️ Some updates failed. Check the logs above for details.")
        logger.info("💡 Network issues are often temporary - try running the script again!")

    return success_count == total_count

if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    incremental = True
    if len(sys.argv) > 1:
        if sys.argv[1] == "--full":
            incremental = False
            print("Running in FULL REFRESH mode")
        elif sys.argv[1] == "--incremental":
            incremental = True
            print("Running in INCREMENTAL mode")
        else:
            print("Usage: python update_active_files.py [--incremental|--full]")
            print("  --incremental: Replace last 2 weeks and add new data (default)")
            print("  --full: Full refresh with all historical data")
            sys.exit(1)
    
    success = main(incremental)
    exit(0 if success else 1)

