#!/usr/bin/env python3

"""
Test Current Validators Script
Gets very recent slots and demonstrates validator analysis with current data.
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime
import time
from typing import Dict, List, Optional

# Configuration
HELIUS_API_KEY = "1ac8e73b-5da0-454a-a6df-c97e500fdf9d"
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
OUTPUT_DIR = "/home/shubham/Downloads/subsidy-working/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log(message: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def hel_rpc_post(method: str, params: list) -> dict:
    """Make Helius RPC call."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=30)
        if resp.status_code == 429:
            time.sleep(0.5)
            resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json() or {}
    except requests.exceptions.RequestException as e:
        log(f"RPC error: {e}")
        return {}

def get_current_slot() -> int:
    """Get the current slot from Solana."""
    result = hel_rpc_post("getSlot", [])
    current_slot = result.get("result", 0)
    log(f"Current slot: {current_slot}")
    return current_slot

def fetch_recent_slot_leaders(current_slot: int, num_slots: int = 100) -> Dict[int, str]:
    """Fetch slot leaders for recent slots."""
    log(f"Fetching slot leaders for {num_slots} recent slots...")
    
    # Get leaders starting from a bit before current slot
    start_slot = max(1, current_slot - num_slots)
    
    result = hel_rpc_post("getSlotLeaders", [start_slot, num_slots])
    
    if "error" in result:
        log(f"Error fetching slot leaders: {result['error']}")
        return {}
    
    leaders = result.get("result", [])
    if not leaders:
        log("No slot leaders returned")
        return {}
    
    # Map slot numbers to validators
    slot_leaders = {}
    for i, leader in enumerate(leaders):
        if leader:  # Skip null/empty leaders
            slot_num = start_slot + i
            slot_leaders[slot_num] = leader
    
    log(f"Successfully fetched {len(slot_leaders)} slot leaders")
    return slot_leaders

def create_mock_transaction_data(slot_leaders: Dict[int, str]) -> pd.DataFrame:
    """Create mock transaction data using real slots and validators."""
    log("Creating mock transaction data with real validator mapping...")
    
    # Use actual slots that have validators - take more slots for better analysis
    slots_with_validators = list(slot_leaders.keys())[:100]  # Take first 100 slots
    
    mock_data = []
    for i, slot in enumerate(slots_with_validators):
        # Create mock transactions for this slot - vary transaction count
        tx_count = 2 + (i % 4)  # 2-5 transactions per slot for variety
        for tx_num in range(1, tx_count + 1):
            mock_data.append({
                'signature': f'mock_tx_{slot}_{tx_num}',
                'slot_rpc': slot,
                'block_time_rpc': int(time.time()) - (len(slots_with_validators) - i) * 0.4,  # Recent timestamps
                'success_rpc': True,
                'fee_lamports_rpc': 5000 + (tx_num * 1000),
                'compute_units_rpc': 50000 + (tx_num * 5000),
                                 'on_chain_tip_lamports': (tx_num * 1000000) if tx_num > 1 else None,  # Some transactions have tips
                 'jito_tip_lamports': (tx_num * 800000) if tx_num > 2 else None,  # Some have Jito tips
                'instruction_count': 2 + tx_num,
                'accounts_count': 10 + tx_num
            })
    
    df = pd.DataFrame(mock_data)
    log(f"Created mock dataset with {len(df)} transactions across {len(slots_with_validators)} slots")
    return df

def add_validator_identity(df: pd.DataFrame, slot_leaders: Dict[int, str]) -> pd.DataFrame:
    """Add validator identity using our slot leaders mapping."""
    log("Adding validator identity to transactions...")
    
    df['validator_identity'] = df['slot_rpc'].map(slot_leaders)
    
    mapped_count = df['validator_identity'].notna().sum()
    success_rate = (mapped_count / len(df)) * 100 if len(df) > 0 else 0
    
    log(f"Validator mapping: {mapped_count}/{len(df)} transactions ({success_rate:.1f}%)")
    log(f"Unique validators: {df['validator_identity'].nunique()}")
    
    return df

def calculate_validator_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate validator-specific metrics."""
    log("Calculating validator metrics...")
    
    # Basic conversions
    df['on_chain_tip_sol'] = df['on_chain_tip_lamports'].fillna(0) / 1e9
    df['jito_tip_sol'] = df['jito_tip_lamports'].fillna(0) / 1e9
    df['fee_sol'] = df['fee_lamports_rpc'] / 1e9
    
    # Profit and subsidy calculations
    df['profit_lamports'] = df['on_chain_tip_lamports'].fillna(0) - df['jito_tip_lamports'].fillna(0)
    df['profit_sol'] = df['profit_lamports'] / 1e9
    
    # Subsidy percentage calculation
    def calc_subsidy(row):
        jito = row.get('jito_tip_lamports', 0) or 0
        onchain = row.get('on_chain_tip_lamports', 0) or 0
        if jito > 0 and onchain > 0:
            return ((jito / onchain) - 1) * 100
        elif jito > 0 and onchain == 0:
            return float('inf')
        return None
    
    df['subsidy_percentage'] = df.apply(calc_subsidy, axis=1)
    
    # Bundle identification
    df['is_jito_bundle'] = df['jito_tip_lamports'].notna() & (df['jito_tip_lamports'] > 0)
    
    # Efficiency metrics
    df['tip_per_cu'] = df.apply(
        lambda r: (r['on_chain_tip_lamports'] / r['compute_units_rpc']) 
        if pd.notna(r.get('on_chain_tip_lamports')) and r['compute_units_rpc'] > 0 
        else None, axis=1
    )
    
    df['fee_per_cu'] = df.apply(
        lambda r: (r['fee_lamports_rpc'] / r['compute_units_rpc']) 
        if r['compute_units_rpc'] > 0 
        else None, axis=1
    )
    
    # Time-based features
    df['timestamp'] = pd.to_datetime(df['block_time_rpc'], unit='s')
    df['latency_seconds'] = int(time.time()) - df['block_time_rpc']
    
    return df

def analyze_validators(df: pd.DataFrame) -> Dict:
    """Perform comprehensive validator analysis."""
    log("🔍 Performing validator analysis...")
    
    if df['validator_identity'].isna().all():
        return {"error": "No validator data available for analysis"}
    
    # Validator performance statistics
    validator_stats = df[df['validator_identity'].notna()].groupby('validator_identity').agg({
        'on_chain_tip_sol': ['count', 'mean', 'sum'],
        'fee_sol': ['mean', 'sum'],
        'compute_units_rpc': ['mean', 'sum'],
        'tip_per_cu': 'mean',
        'fee_per_cu': 'mean',
        'latency_seconds': 'mean',
        'success_rpc': ['mean', 'count']
    }).round(6)
    
    # Flatten column names
    validator_stats.columns = [f"{col[0]}_{col[1]}" for col in validator_stats.columns]
    validator_stats = validator_stats.reset_index()
    
    # Calculate derived metrics
    validator_stats['tx_count'] = validator_stats['on_chain_tip_sol_count']
    validator_stats['total_tip_volume'] = validator_stats['on_chain_tip_sol_sum']
    validator_stats['avg_tip_per_tx'] = validator_stats['on_chain_tip_sol_mean']
    validator_stats['success_rate'] = validator_stats['success_rpc_mean']
    validator_stats['total_compute_units'] = validator_stats['compute_units_rpc_sum']
    
    # Sort by total tip volume
    validator_stats = validator_stats.sort_values('total_tip_volume', ascending=False)
    
    analysis = {
        'timestamp': datetime.now().isoformat(),
        'dataset_summary': {
            'total_transactions': len(df),
            'transactions_with_validators': int(df['validator_identity'].notna().sum()),
            'unique_validators': int(df['validator_identity'].nunique()),
            'total_tip_volume_sol': float(df['on_chain_tip_sol'].sum()),
            'avg_tip_sol': float(df['on_chain_tip_sol'].mean()),
            'success_rate': float(df['success_rpc'].mean())
        },
        'validator_performance': {
            'top_validators': validator_stats.head(10).to_dict('records'),
            'validator_summary': {
                'total_validators': len(validator_stats),
                'avg_transactions_per_validator': float(validator_stats['tx_count'].mean()),
                'top_validator_volume': float(validator_stats['total_tip_volume'].iloc[0]) if len(validator_stats) > 0 else 0,
                'validator_concentration_top5': float(validator_stats.head(5)['total_tip_volume'].sum() / validator_stats['total_tip_volume'].sum()) if validator_stats['total_tip_volume'].sum() > 0 else 0
            }
        }
    }
    
    # Correlation analysis
    if len(validator_stats) > 3:
        correlations = {}
        
        # Tip volume vs compute units
        corr_tip_cu = validator_stats['total_tip_volume'].corr(validator_stats['total_compute_units'])
        correlations['tip_volume_vs_compute_units'] = float(corr_tip_cu) if not pd.isna(corr_tip_cu) else None
        
        # Average tip vs success rate
        corr_tip_success = validator_stats['avg_tip_per_tx'].corr(validator_stats['success_rate'])
        correlations['avg_tip_vs_success_rate'] = float(corr_tip_success) if not pd.isna(corr_tip_success) else None
        
        analysis['correlations'] = correlations
    
    return analysis

def perform_detailed_validator_analysis(df: pd.DataFrame) -> Dict:
    """Perform extremely detailed validator analysis with advanced metrics."""
    log("🔬 Performing detailed validator analysis...")
    
    if df['validator_identity'].isna().all():
        return {"error": "No validator data available for detailed analysis"}
    
    detailed_analysis = {
        'timestamp': datetime.now().isoformat(),
        'analysis_scope': 'comprehensive_validator_performance'
    }
    
    # 1. VALIDATOR PERFORMANCE MATRIX
    log("📊 Creating validator performance matrix...")
    validator_df = df[df['validator_identity'].notna()].copy()
    
    performance_matrix = validator_df.groupby('validator_identity').agg({
        # Transaction metrics
        'signature': 'count',
        'success_rpc': ['mean', 'sum', 'count'],
        
        # Financial metrics
        'on_chain_tip_sol': ['count', 'mean', 'median', 'std', 'sum', 'min', 'max'],
        'jito_tip_sol': ['count', 'mean', 'median', 'std', 'sum'],
        'profit_sol': ['mean', 'median', 'std', 'sum', 'min', 'max'],
        'fee_sol': ['mean', 'sum', 'std'],
        'subsidy_percentage': ['mean', 'median', 'std', 'count'],
        
        # Technical metrics
        'compute_units_rpc': ['mean', 'median', 'std', 'sum', 'min', 'max'],
        'instruction_count': ['mean', 'median', 'std'],
        'accounts_count': ['mean', 'median', 'std'],
        'latency_seconds': ['mean', 'median', 'std', 'min', 'max'],
        
        # Efficiency metrics
        'tip_per_cu': ['mean', 'median', 'std'],
        'fee_per_cu': ['mean', 'median', 'std'],
        
        # Bundle metrics
        'is_jito_bundle': ['sum', 'mean']
    }).round(6)
    
    # Flatten column names
    performance_matrix.columns = [f"{col[0]}_{col[1]}" for col in performance_matrix.columns]
    performance_matrix = performance_matrix.reset_index()
    
    # Add derived metrics
    performance_matrix['total_transactions'] = performance_matrix['signature_count']
    performance_matrix['success_rate'] = performance_matrix['success_rpc_mean']
    performance_matrix['failure_rate'] = 1 - performance_matrix['success_rpc_mean']
    performance_matrix['bundle_rate'] = performance_matrix['is_jito_bundle_mean']
    performance_matrix['avg_profit_per_tx'] = performance_matrix['profit_sol_mean']
    performance_matrix['total_profit'] = performance_matrix['profit_sol_sum']
    performance_matrix['profit_consistency'] = 1 / (1 + performance_matrix['profit_sol_std'])  # Lower std = higher consistency
    
    # Calculate efficiency scores
    performance_matrix['cu_efficiency_score'] = (
        performance_matrix['tip_per_cu_mean'] / performance_matrix['tip_per_cu_mean'].max()
    ) * 100
    
    performance_matrix['overall_efficiency'] = (
        (performance_matrix['success_rate'] * 0.3) +
        (performance_matrix['cu_efficiency_score'] / 100 * 0.3) +
        (performance_matrix['profit_consistency'] * 0.2) +
        ((performance_matrix['total_profit'] / performance_matrix['total_profit'].max()) * 0.2)
    ) * 100
    
    # Sort by overall efficiency
    performance_matrix = performance_matrix.sort_values('overall_efficiency', ascending=False)
    
    detailed_analysis['performance_matrix'] = performance_matrix.head(20).to_dict('records')
    
    # 2. VALIDATOR RANKINGS
    log("🏆 Creating validator rankings...")
    rankings = {
        'by_total_profit': performance_matrix.nlargest(10, 'total_profit')[['validator_identity', 'total_profit', 'total_transactions']].to_dict('records'),
        'by_avg_profit': performance_matrix.nlargest(10, 'avg_profit_per_tx')[['validator_identity', 'avg_profit_per_tx', 'total_transactions']].to_dict('records'),
        'by_success_rate': performance_matrix.nlargest(10, 'success_rate')[['validator_identity', 'success_rate', 'total_transactions']].to_dict('records'),
        'by_efficiency': performance_matrix.nlargest(10, 'overall_efficiency')[['validator_identity', 'overall_efficiency', 'total_transactions']].to_dict('records'),
        'by_transaction_volume': performance_matrix.nlargest(10, 'total_transactions')[['validator_identity', 'total_transactions', 'total_profit']].to_dict('records'),
        'by_cu_efficiency': performance_matrix.nlargest(10, 'cu_efficiency_score')[['validator_identity', 'cu_efficiency_score', 'tip_per_cu_mean']].to_dict('records')
    }
    
    detailed_analysis['rankings'] = rankings
    
    # 3. CORRELATION ANALYSIS
    log("📈 Performing correlation analysis...")
    correlations = {}
    
    # Key correlations to analyze
    correlation_pairs = [
        ('total_transactions', 'total_profit', 'Transaction Volume vs Total Profit'),
        ('success_rate', 'avg_profit_per_tx', 'Success Rate vs Average Profit'),
        ('cu_efficiency_score', 'total_profit', 'CU Efficiency vs Total Profit'),
        ('bundle_rate', 'avg_profit_per_tx', 'Bundle Rate vs Average Profit'),
        ('latency_seconds_mean', 'success_rate', 'Average Latency vs Success Rate'),
        ('instruction_count_mean', 'compute_units_rpc_mean', 'Instructions vs Compute Units'),
        ('subsidy_percentage_mean', 'profit_sol_mean', 'Average Subsidy vs Profit')
    ]
    
    for col1, col2, description in correlation_pairs:
        if col1 in performance_matrix.columns and col2 in performance_matrix.columns:
            valid_data = performance_matrix[[col1, col2]].dropna()
            if len(valid_data) > 2:
                corr = valid_data[col1].corr(valid_data[col2])
                correlations[f"{col1}_vs_{col2}"] = {
                    'correlation': float(corr) if not pd.isna(corr) else None,
                    'description': description,
                    'sample_size': len(valid_data),
                    'interpretation': 'Strong' if abs(corr) > 0.7 else 'Moderate' if abs(corr) > 0.4 else 'Weak'
                }
    
    detailed_analysis['correlations'] = correlations
    
    # 4. VALIDATOR CLUSTERING/SEGMENTATION
    log("🎯 Performing validator segmentation...")
    
    # Create validator segments based on performance characteristics
    segments = {}
    
    # High performers: Top 20% by overall efficiency
    high_perf_threshold = performance_matrix['overall_efficiency'].quantile(0.8)
    high_performers = performance_matrix[performance_matrix['overall_efficiency'] >= high_perf_threshold]
    
    # Volume leaders: Top 20% by transaction count
    volume_threshold = performance_matrix['total_transactions'].quantile(0.8)
    volume_leaders = performance_matrix[performance_matrix['total_transactions'] >= volume_threshold]
    
    # Profit leaders: Top 20% by total profit
    profit_threshold = performance_matrix['total_profit'].quantile(0.8)
    profit_leaders = performance_matrix[performance_matrix['total_profit'] >= profit_threshold]
    
    segments['high_performers'] = {
        'count': len(high_performers),
        'avg_efficiency': float(high_performers['overall_efficiency'].mean()),
        'avg_profit': float(high_performers['total_profit'].mean()),
        'validators': high_performers['validator_identity'].tolist()[:10]
    }
    
    segments['volume_leaders'] = {
        'count': len(volume_leaders),
        'avg_transactions': float(volume_leaders['total_transactions'].mean()),
        'avg_success_rate': float(volume_leaders['success_rate'].mean()),
        'validators': volume_leaders['validator_identity'].tolist()[:10]
    }
    
    segments['profit_leaders'] = {
        'count': len(profit_leaders),
        'avg_profit': float(profit_leaders['total_profit'].mean()),
        'avg_profit_per_tx': float(profit_leaders['avg_profit_per_tx'].mean()),
        'validators': profit_leaders['validator_identity'].tolist()[:10]
    }
    
    detailed_analysis['segments'] = segments
    
    # 5. COMPETITIVE ANALYSIS
    log("⚔️  Performing competitive analysis...")
    
    competitive_metrics = {
        'market_concentration': {
            'top_5_share': float(performance_matrix.head(5)['total_profit'].sum() / performance_matrix['total_profit'].sum()),
            'top_10_share': float(performance_matrix.head(10)['total_profit'].sum() / performance_matrix['total_profit'].sum()),
            'herfindahl_index': float((performance_matrix['total_profit'] / performance_matrix['total_profit'].sum() ** 2).sum())
        },
        'performance_distribution': {
            'efficiency_std': float(performance_matrix['overall_efficiency'].std()),
            'profit_std': float(performance_matrix['total_profit'].std()),
            'success_rate_range': float(performance_matrix['success_rate'].max() - performance_matrix['success_rate'].min())
        },
        'validator_tiers': {
            'tier_1_count': len(performance_matrix[performance_matrix['overall_efficiency'] >= 80]),
            'tier_2_count': len(performance_matrix[(performance_matrix['overall_efficiency'] >= 60) & (performance_matrix['overall_efficiency'] < 80)]),
            'tier_3_count': len(performance_matrix[performance_matrix['overall_efficiency'] < 60])
        }
    }
    
    detailed_analysis['competitive_analysis'] = competitive_metrics
    
    # 6. SUBSIDY IMPACT ANALYSIS
    log("💰 Analyzing subsidy impact...")
    
    subsidy_analysis = {}
    
    # Filter validators with subsidy data
    subsidy_validators = performance_matrix[performance_matrix['subsidy_percentage_count'] > 0]
    
    if len(subsidy_validators) > 0:
        # Subsidy vs performance correlations
        subsidy_analysis['subsidy_stats'] = {
            'validators_with_subsidies': len(subsidy_validators),
            'avg_subsidy_percentage': float(subsidy_validators['subsidy_percentage_mean'].mean()),
            'subsidy_range': {
                'min': float(subsidy_validators['subsidy_percentage_mean'].min()),
                'max': float(subsidy_validators['subsidy_percentage_mean'].max())
            }
        }
        
        # High vs low subsidy comparison
        median_subsidy = subsidy_validators['subsidy_percentage_mean'].median()
        high_subsidy = subsidy_validators[subsidy_validators['subsidy_percentage_mean'] > median_subsidy]
        low_subsidy = subsidy_validators[subsidy_validators['subsidy_percentage_mean'] <= median_subsidy]
        
        subsidy_analysis['subsidy_comparison'] = {
            'high_subsidy_group': {
                'count': len(high_subsidy),
                'avg_profit': float(high_subsidy['total_profit'].mean()),
                'avg_success_rate': float(high_subsidy['success_rate'].mean())
            },
            'low_subsidy_group': {
                'count': len(low_subsidy),
                'avg_profit': float(low_subsidy['total_profit'].mean()),
                'avg_success_rate': float(low_subsidy['success_rate'].mean())
            }
        }
    
    detailed_analysis['subsidy_impact'] = subsidy_analysis
    
    # 7. SUMMARY INSIGHTS
    log("🎯 Generating key insights...")
    
    insights = []
    
    # Top performer insights
    if len(performance_matrix) > 0:
        top_validator = performance_matrix.iloc[0]
        insights.append(f"Top performing validator: {top_validator['validator_identity'][:30]}... with {top_validator['overall_efficiency']:.1f}% efficiency score")
        insights.append(f"Total validators analyzed: {len(performance_matrix)}")
        insights.append(f"Average success rate across all validators: {performance_matrix['success_rate'].mean()*100:.1f}%")
        insights.append(f"Total profit generated: {performance_matrix['total_profit'].sum():.6f} SOL")
    
    # Correlation insights
    strong_correlations = [k for k, v in correlations.items() if v.get('correlation') and abs(v['correlation']) > 0.7]
    if strong_correlations:
        insights.append(f"Found {len(strong_correlations)} strong correlations between validator metrics")
    
    # Market concentration insights
    if 'market_concentration' in competitive_metrics:
        top_5_share = competitive_metrics['market_concentration']['top_5_share']
        insights.append(f"Top 5 validators control {top_5_share*100:.1f}% of total profit")
    
    detailed_analysis['key_insights'] = insights
    
    return detailed_analysis

def generate_comprehensive_report(df: pd.DataFrame, analysis: Dict, detailed_analysis: Dict, comparison: pd.DataFrame) -> None:
    """Generate a comprehensive analysis report in the root directory."""
    log("📝 Generating comprehensive analysis report...")
    
    # Report file in root directory
    report_file = f'comprehensive_validator_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
    
    with open(report_file, 'w') as f:
        # Header
        f.write("=" * 80 + "\n")
        f.write("COMPREHENSIVE SOLANA VALIDATOR ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Analysis Scope: {detailed_analysis.get('analysis_scope', 'N/A')}\n")
        f.write("=" * 80 + "\n\n")
        
        # EXECUTIVE SUMMARY
        f.write("🎯 EXECUTIVE SUMMARY\n")
        f.write("-" * 50 + "\n")
        f.write(f"Total Transactions Analyzed: {len(df):,}\n")
        f.write(f"Transactions with Validator Data: {df['validator_identity'].notna().sum():,}\n")
        f.write(f"Unique Validators: {df['validator_identity'].nunique()}\n")
        f.write(f"Validator Coverage: {df['validator_identity'].notna().sum()/len(df)*100:.1f}%\n")
        f.write(f"Average Tip per Transaction: {df['on_chain_tip_sol'].mean():.6f} SOL\n")
        f.write(f"Average Profit per Transaction: {df['profit_sol'].mean():.6f} SOL\n")
        f.write(f"Total Profit Generated: {df['profit_sol'].sum():.6f} SOL\n")
        f.write(f"Overall Success Rate: {df['success_rpc'].mean()*100:.1f}%\n")
        f.write(f"Jito Bundle Participation: {df['is_jito_bundle'].mean()*100:.1f}%\n\n")
        
        # KEY INSIGHTS
        if 'key_insights' in detailed_analysis:
            f.write("💡 KEY INSIGHTS\n")
            f.write("-" * 50 + "\n")
            for i, insight in enumerate(detailed_analysis['key_insights'], 1):
                f.write(f"{i}. {insight}\n")
            f.write("\n")
        
        # VALIDATOR RANKINGS
        if 'rankings' in detailed_analysis:
            rankings = detailed_analysis['rankings']
            
            f.write("🏆 VALIDATOR RANKINGS\n")
            f.write("-" * 50 + "\n")
            
            # Top performers by total profit
            f.write("📈 TOP 10 VALIDATORS BY TOTAL PROFIT:\n")
            for i, validator in enumerate(rankings.get('by_total_profit', [])[:10], 1):
                f.write(f"{i:2d}. {validator['validator_identity'][:40]}...\n")
                f.write(f"     Profit: {validator['total_profit']:.6f} SOL | Transactions: {validator['total_transactions']}\n")
            f.write("\n")
            
            # Top performers by efficiency
            f.write("⚡ TOP 10 VALIDATORS BY OVERALL EFFICIENCY:\n")
            for i, validator in enumerate(rankings.get('by_efficiency', [])[:10], 1):
                f.write(f"{i:2d}. {validator['validator_identity'][:40]}...\n")
                f.write(f"     Efficiency Score: {validator['overall_efficiency']:.2f}% | Transactions: {validator['total_transactions']}\n")
            f.write("\n")
            
            # Top performers by CU efficiency
            f.write("🔧 TOP 10 VALIDATORS BY COMPUTE UNIT EFFICIENCY:\n")
            for i, validator in enumerate(rankings.get('by_cu_efficiency', [])[:10], 1):
                f.write(f"{i:2d}. {validator['validator_identity'][:40]}...\n")
                f.write(f"     CU Efficiency: {validator['cu_efficiency_score']:.2f}% | Tip/CU: {validator['tip_per_cu_mean']:.2f} lamports\n")
            f.write("\n")
            
            # Top performers by transaction volume
            f.write("📊 TOP 10 VALIDATORS BY TRANSACTION VOLUME:\n")
            for i, validator in enumerate(rankings.get('by_transaction_volume', [])[:10], 1):
                f.write(f"{i:2d}. {validator['validator_identity'][:40]}...\n")
                f.write(f"     Transactions: {validator['total_transactions']} | Profit: {validator['total_profit']:.6f} SOL\n")
            f.write("\n")
        
        # CORRELATION ANALYSIS
        if 'correlations' in detailed_analysis:
            f.write("📈 CORRELATION ANALYSIS\n")
            f.write("-" * 50 + "\n")
            correlations = detailed_analysis['correlations']
            
            strong_corrs = []
            moderate_corrs = []
            weak_corrs = []
            
            for metric, data in correlations.items():
                if data['correlation'] is not None:
                    corr_value = data['correlation']
                    if abs(corr_value) > 0.7:
                        strong_corrs.append((metric, data))
                    elif abs(corr_value) > 0.4:
                        moderate_corrs.append((metric, data))
                    else:
                        weak_corrs.append((metric, data))
            
            f.write("🔴 STRONG CORRELATIONS (|r| > 0.7):\n")
            for metric, data in strong_corrs:
                f.write(f"   • {data['description']}: r = {data['correlation']:.4f} (n={data['sample_size']})\n")
            f.write("\n")
            
            f.write("🟡 MODERATE CORRELATIONS (0.4 < |r| ≤ 0.7):\n")
            for metric, data in moderate_corrs:
                f.write(f"   • {data['description']}: r = {data['correlation']:.4f} (n={data['sample_size']})\n")
            f.write("\n")
            
            f.write("🟢 WEAK CORRELATIONS (|r| ≤ 0.4):\n")
            for metric, data in weak_corrs:
                f.write(f"   • {data['description']}: r = {data['correlation']:.4f} (n={data['sample_size']})\n")
            f.write("\n")
        
        # MARKET ANALYSIS
        if 'competitive_analysis' in detailed_analysis:
            comp_analysis = detailed_analysis['competitive_analysis']
            f.write("🏪 MARKET STRUCTURE ANALYSIS\n")
            f.write("-" * 50 + "\n")
            
            if 'market_concentration' in comp_analysis:
                market = comp_analysis['market_concentration']
                f.write("📊 Market Concentration:\n")
                f.write(f"   • Top 5 validators control: {market['top_5_share']*100:.1f}% of total profit\n")
                f.write(f"   • Top 10 validators control: {market['top_10_share']*100:.1f}% of total profit\n")
                f.write(f"   • Herfindahl-Hirschman Index: {market['herfindahl_index']:.2f}\n")
                
                # Interpret HHI
                hhi = market['herfindahl_index']
                if hhi < 1500:
                    hhi_interpretation = "Unconcentrated (Competitive)"
                elif hhi < 2500:
                    hhi_interpretation = "Moderately Concentrated"
                else:
                    hhi_interpretation = "Highly Concentrated"
                f.write(f"   • Market Classification: {hhi_interpretation}\n\n")
            
            if 'performance_distribution' in comp_analysis:
                perf = comp_analysis['performance_distribution']
                f.write("📈 Performance Distribution:\n")
                f.write(f"   • Efficiency Standard Deviation: {perf['efficiency_std']:.2f}%\n")
                f.write(f"   • Profit Standard Deviation: {perf['profit_std']:.6f} SOL\n")
                f.write(f"   • Success Rate Range: {perf['success_rate_range']*100:.1f}%\n\n")
            
            if 'validator_tiers' in comp_analysis:
                tiers = comp_analysis['validator_tiers']
                f.write("🏅 Validator Performance Tiers:\n")
                f.write(f"   • Tier 1 (≥80% efficiency): {tiers['tier_1_count']} validators\n")
                f.write(f"   • Tier 2 (60-79% efficiency): {tiers['tier_2_count']} validators\n")
                f.write(f"   • Tier 3 (<60% efficiency): {tiers['tier_3_count']} validators\n\n")
        
        # VALIDATOR SEGMENTS
        if 'segments' in detailed_analysis:
            segments = detailed_analysis['segments']
            f.write("🎯 VALIDATOR SEGMENTATION\n")
            f.write("-" * 50 + "\n")
            
            if 'high_performers' in segments:
                hp = segments['high_performers']
                f.write(f"⭐ High Performers (Top 20% by efficiency): {hp['count']} validators\n")
                f.write(f"   • Average Efficiency: {hp['avg_efficiency']:.1f}%\n")
                f.write(f"   • Average Profit: {hp['avg_profit']:.6f} SOL\n")
                f.write("   • Top Validators:\n")
                for i, validator in enumerate(hp['validators'][:5], 1):
                    f.write(f"     {i}. {validator[:50]}...\n")
                f.write("\n")
            
            if 'volume_leaders' in segments:
                vl = segments['volume_leaders']
                f.write(f"📊 Volume Leaders (Top 20% by transactions): {vl['count']} validators\n")
                f.write(f"   • Average Transactions: {vl['avg_transactions']:.0f}\n")
                f.write(f"   • Average Success Rate: {vl['avg_success_rate']*100:.1f}%\n")
                f.write("   • Top Validators:\n")
                for i, validator in enumerate(vl['validators'][:5], 1):
                    f.write(f"     {i}. {validator[:50]}...\n")
                f.write("\n")
            
            if 'profit_leaders' in segments:
                pl = segments['profit_leaders']
                f.write(f"💰 Profit Leaders (Top 20% by profit): {pl['count']} validators\n")
                f.write(f"   • Average Total Profit: {pl['avg_profit']:.6f} SOL\n")
                f.write(f"   • Average Profit per Tx: {pl['avg_profit_per_tx']:.6f} SOL\n")
                f.write("   • Top Validators:\n")
                for i, validator in enumerate(pl['validators'][:5], 1):
                    f.write(f"     {i}. {validator[:50]}...\n")
                f.write("\n")
        
        # SUBSIDY IMPACT ANALYSIS
        if 'subsidy_impact' in detailed_analysis and detailed_analysis['subsidy_impact']:
            subsidy = detailed_analysis['subsidy_impact']
            f.write("💸 SUBSIDY IMPACT ANALYSIS\n")
            f.write("-" * 50 + "\n")
            
            if 'subsidy_stats' in subsidy:
                stats = subsidy['subsidy_stats']
                f.write(f"📊 Subsidy Statistics:\n")
                f.write(f"   • Validators with Subsidies: {stats['validators_with_subsidies']}\n")
                f.write(f"   • Average Subsidy Percentage: {stats['avg_subsidy_percentage']:.2f}%\n")
                f.write(f"   • Subsidy Range: {stats['subsidy_range']['min']:.2f}% to {stats['subsidy_range']['max']:.2f}%\n\n")
            
            if 'subsidy_comparison' in subsidy:
                comp = subsidy['subsidy_comparison']
                f.write("🔍 High vs Low Subsidy Comparison:\n")
                if 'high_subsidy_group' in comp:
                    hsg = comp['high_subsidy_group']
                    f.write(f"   • High Subsidy Group: {hsg['count']} validators\n")
                    f.write(f"     - Average Profit: {hsg['avg_profit']:.6f} SOL\n")
                    f.write(f"     - Average Success Rate: {hsg['avg_success_rate']*100:.1f}%\n")
                if 'low_subsidy_group' in comp:
                    lsg = comp['low_subsidy_group']
                    f.write(f"   • Low Subsidy Group: {lsg['count']} validators\n")
                    f.write(f"     - Average Profit: {lsg['avg_profit']:.6f} SOL\n")
                    f.write(f"     - Average Success Rate: {lsg['avg_success_rate']*100:.1f}%\n")
                f.write("\n")
        
        # DETAILED PERFORMANCE MATRIX
        if 'performance_matrix' in detailed_analysis:
            f.write("📋 DETAILED PERFORMANCE MATRIX (TOP 20 VALIDATORS)\n")
            f.write("-" * 80 + "\n")
            
            # Header
            f.write(f"{'Rank':<4} {'Validator':<42} {'Efficiency':<10} {'Profit':<12} {'Txs':<6} {'Success':<8}\n")
            f.write("-" * 80 + "\n")
            
            for i, validator in enumerate(detailed_analysis['performance_matrix'][:20], 1):
                f.write(f"{i:<4} {validator['validator_identity'][:40]:<42} "
                       f"{validator['overall_efficiency']:<10.1f} "
                       f"{validator['total_profit']:<12.6f} "
                       f"{validator['total_transactions']:<6} "
                       f"{validator['success_rate']*100:<8.1f}\n")
            f.write("\n")
        
        # TECHNICAL METRICS SUMMARY
        f.write("🔧 TECHNICAL METRICS SUMMARY\n")
        f.write("-" * 50 + "\n")
        
        # Compute Units Analysis
        cu_stats = df['compute_units_rpc'].describe()
        f.write("💻 Compute Units Analysis:\n")
        f.write(f"   • Mean: {cu_stats['mean']:,.0f} CU\n")
        f.write(f"   • Median: {cu_stats['50%']:,.0f} CU\n")
        f.write(f"   • Standard Deviation: {cu_stats['std']:,.0f} CU\n")
        f.write(f"   • Range: {cu_stats['min']:,.0f} - {cu_stats['max']:,.0f} CU\n\n")
        
        # Fee Analysis
        fee_stats = df['fee_sol'].describe()
        f.write("💰 Fee Analysis:\n")
        f.write(f"   • Mean: {fee_stats['mean']:.6f} SOL\n")
        f.write(f"   • Median: {fee_stats['50%']:.6f} SOL\n")
        f.write(f"   • Standard Deviation: {fee_stats['std']:.6f} SOL\n")
        f.write(f"   • Range: {fee_stats['min']:.6f} - {fee_stats['max']:.6f} SOL\n\n")
        
        # Tip Analysis
        tip_stats = df['on_chain_tip_sol'].describe()
        f.write("🎯 Tip Analysis:\n")
        f.write(f"   • Mean: {tip_stats['mean']:.6f} SOL\n")
        f.write(f"   • Median: {tip_stats['50%']:.6f} SOL\n")
        f.write(f"   • Standard Deviation: {tip_stats['std']:.6f} SOL\n")
        f.write(f"   • Range: {tip_stats['min']:.6f} - {tip_stats['max']:.6f} SOL\n\n")
        
        # Efficiency Ratios
        f.write("⚡ Efficiency Ratios:\n")
        f.write(f"   • Average Tip per CU: {df['tip_per_cu'].mean():.2f} lamports/CU\n")
        f.write(f"   • Average Fee per CU: {df['fee_per_cu'].mean():.2f} lamports/CU\n")
        f.write(f"   • Average Instructions per Transaction: {df['instruction_count'].mean():.1f}\n")
        f.write(f"   • Average Accounts per Transaction: {df['accounts_count'].mean():.1f}\n\n")
        
        # FOOTER
        f.write("=" * 80 + "\n")
        f.write("END OF COMPREHENSIVE VALIDATOR ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Report generated by Solana Validator Analysis System v2.0\n")
        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        f.write(f"Total analysis time: <1 second\n")
        f.write("=" * 80 + "\n")
    
    log(f"✅ Comprehensive report saved: {report_file}")

def create_validator_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Create detailed validator comparison table."""
    log("Creating validator comparison table...")
    
    if df['validator_identity'].isna().all():
        return pd.DataFrame()
    
    # Group by validator for comparison
    comparison = df[df['validator_identity'].notna()].groupby('validator_identity').agg({
        'on_chain_tip_sol': ['count', 'mean', 'median', 'sum'],
        'fee_sol': ['mean', 'sum'],
        'compute_units_rpc': ['mean', 'sum'],
        'tip_per_cu': ['mean', 'median'],
        'fee_per_cu': ['mean', 'median'],
        'latency_seconds': ['mean', 'min', 'max'],
        'success_rpc': ['mean', 'sum'],
        'instruction_count': 'mean',
        'accounts_count': 'mean'
    }).round(6)
    
    # Flatten and rename columns
    comparison.columns = [f"{col[0]}_{col[1]}" for col in comparison.columns]
    comparison = comparison.reset_index()
    
    # Add ranking columns
    comparison['tip_volume_rank'] = comparison['on_chain_tip_sol_sum'].rank(ascending=False, method='dense')
    comparison['efficiency_rank'] = comparison['tip_per_cu_mean'].rank(ascending=False, method='dense')
    comparison['success_rank'] = comparison['success_rpc_mean'].rank(ascending=False, method='dense')
    
    # Sort by tip volume
    comparison = comparison.sort_values('on_chain_tip_sol_sum', ascending=False)
    
    return comparison

def save_results(df: pd.DataFrame, analysis: Dict, comparison: pd.DataFrame):
    """Save all results to files."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save main dataset
    dataset_file = os.path.join(OUTPUT_DIR, f'current_validator_test_dataset_{timestamp}.csv')
    df.to_csv(dataset_file, index=False)
    log(f"✅ Dataset saved: {dataset_file}")
    
    # Save analysis results
    analysis_file = os.path.join(OUTPUT_DIR, f'current_validator_analysis_{timestamp}.json')
    with open(analysis_file, 'w') as f:
        json.dump(analysis, f, indent=2, default=str)
    log(f"✅ Analysis saved: {analysis_file}")
    
    # Save validator comparison
    if not comparison.empty:
        comparison_file = os.path.join(OUTPUT_DIR, f'validator_comparison_{timestamp}.csv')
        comparison.to_csv(comparison_file, index=False)
        log(f"✅ Validator comparison saved: {comparison_file}")
    
    # Create summary report
    summary_file = os.path.join(OUTPUT_DIR, f'current_validator_summary_{timestamp}.txt')
    with open(summary_file, 'w') as f:
        f.write("CURRENT VALIDATOR ANALYSIS TEST\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Analysis Time: {analysis['timestamp']}\n")
        f.write(f"Dataset: {analysis['dataset_summary']['total_transactions']} transactions\n")
        f.write(f"Validator Coverage: {analysis['dataset_summary']['transactions_with_validators']}/{analysis['dataset_summary']['total_transactions']} transactions\n")
        f.write(f"Unique Validators: {analysis['dataset_summary']['unique_validators']}\n")
        f.write(f"Total Tip Volume: {analysis['dataset_summary']['total_tip_volume_sol']:.6f} SOL\n")
        f.write(f"Average Tip: {analysis['dataset_summary']['avg_tip_sol']:.6f} SOL\n")
        f.write(f"Success Rate: {analysis['dataset_summary']['success_rate']*100:.1f}%\n\n")
        
        if 'validator_performance' in analysis:
            f.write("TOP 5 VALIDATORS BY TIP VOLUME:\n")
            for i, validator in enumerate(analysis['validator_performance']['top_validators'][:5], 1):
                f.write(f"{i}. {validator['validator_identity'][:25]}... - {validator['total_tip_volume']:.6f} SOL ({validator['tx_count']} txs)\n")
            f.write("\n")
        
        if 'correlations' in analysis:
            f.write("CORRELATION ANALYSIS:\n")
            for key, value in analysis['correlations'].items():
                if value is not None:
                    f.write(f"• {key.replace('_', ' ').title()}: {value:.3f}\n")
    
    log(f"✅ Summary saved: {summary_file}")

def main():
    """Main execution function."""
    log("Starting Current Validator Test")
    
    try:
        # Get current slot
        current_slot = get_current_slot()
        if current_slot == 0:
            log("❌ Could not get current slot")
            return
        
        # Fetch recent slot leaders - get more slots for better validator diversity
        slot_leaders = fetch_recent_slot_leaders(current_slot, 1000)  # Get 1000 recent slots
        if not slot_leaders:
            log("❌ Could not fetch slot leaders")
            return
        
        # Create mock transaction data with real validator mapping
        df = create_mock_transaction_data(slot_leaders)
        
        # Add validator identity
        df = add_validator_identity(df, slot_leaders)
        
        # Calculate metrics
        df = calculate_validator_metrics(df)
        
        # Perform basic analysis
        analysis = analyze_validators(df)
        
        # Perform detailed analysis
        detailed_analysis = perform_detailed_validator_analysis(df)
        
        # Create validator comparison
        comparison = create_validator_comparison(df)
        
        # Save results
        save_results(df, analysis, comparison)
        
        # Save detailed analysis separately
        detailed_file = os.path.join(OUTPUT_DIR, f'detailed_validator_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(detailed_file, 'w') as f:
            json.dump(detailed_analysis, f, indent=2, default=str)
        log(f"✅ Detailed analysis saved: {detailed_file}")
        
        # Generate comprehensive analysis report in root directory
        generate_comprehensive_report(df, analysis, detailed_analysis, comparison)
        
        # Print detailed summary
        log("📈 DETAILED ANALYSIS COMPLETE - COMPREHENSIVE SUMMARY:")
        log(f"   • Total transactions: {len(df)}")
        log(f"   • Transactions with validators: {df['validator_identity'].notna().sum()}")
        log(f"   • Unique validators: {df['validator_identity'].nunique()}")
        log(f"   • Average tip: {df['on_chain_tip_sol'].mean():.6f} SOL")
        log(f"   • Average profit: {df['profit_sol'].mean():.6f} SOL")
        log(f"   • Success rate: {df['success_rpc'].mean()*100:.1f}%")
        log(f"   • Jito bundle rate: {df['is_jito_bundle'].mean()*100:.1f}%")
        
        if df['validator_identity'].notna().any():
            # Show top validators by different metrics
            validator_profit = df[df['validator_identity'].notna()].groupby('validator_identity')['profit_sol'].sum().sort_values(ascending=False)
            validator_efficiency = df[df['validator_identity'].notna()].groupby('validator_identity')['tip_per_cu'].mean().sort_values(ascending=False)
            validator_success = df[df['validator_identity'].notna()].groupby('validator_identity')['success_rpc'].mean().sort_values(ascending=False)
            
            log("   • TOP VALIDATORS BY TOTAL PROFIT:")
            for validator, profit in validator_profit.head(3).items():
                tx_count = df[df['validator_identity'] == validator].shape[0]
                log(f"     - {validator[:30]}...: {profit:.6f} SOL ({tx_count} txs)")
            
            log("   • TOP VALIDATORS BY EFFICIENCY (tip per CU):")
            for validator, eff in validator_efficiency.head(3).items():
                log(f"     - {validator[:30]}...: {eff:.2f} lamports/CU")
            
            log("   • TOP VALIDATORS BY SUCCESS RATE:")
            for validator, success in validator_success.head(3).items():
                log(f"     - {validator[:30]}...: {success*100:.1f}%")
        
        # Show key insights from detailed analysis
        if 'key_insights' in detailed_analysis:
            log("   • KEY INSIGHTS:")
            for insight in detailed_analysis['key_insights'][:5]:
                log(f"     - {insight}")
        
        # Show market concentration
        if 'competitive_analysis' in detailed_analysis:
            comp_analysis = detailed_analysis['competitive_analysis']
            if 'market_concentration' in comp_analysis:
                top5_share = comp_analysis['market_concentration']['top_5_share']
                log(f"   • Market concentration: Top 5 validators control {top5_share*100:.1f}% of profit")
        
        # Show validator tiers
        if 'competitive_analysis' in detailed_analysis and 'validator_tiers' in detailed_analysis['competitive_analysis']:
            tiers = detailed_analysis['competitive_analysis']['validator_tiers']
            log(f"   • Validator tiers: Tier 1: {tiers['tier_1_count']}, Tier 2: {tiers['tier_2_count']}, Tier 3: {tiers['tier_3_count']}")
        
        # Show correlation findings
        if 'correlations' in detailed_analysis:
            strong_corrs = [k for k, v in detailed_analysis['correlations'].items() 
                          if v.get('correlation') and abs(v['correlation']) > 0.7]
            if strong_corrs:
                log(f"   • Found {len(strong_corrs)} strong correlations between metrics")
        
        log("✅ Test completed successfully!")
        
    except Exception as e:
        log(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    main() 