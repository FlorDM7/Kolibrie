import pandas as pd
import matplotlib.pyplot as plt
import os
import math
import argparse

# Global flag to control whether to call interactive plt.show()
SHOW_PLOTS = False

def make_avg_plot_by_window_size(title, df1, df2, df3, df4, filename=None, show_error_bars=False):
    bin_size = 25
    max_ticks = 12
    min_bin_count = 50  # Filter out bins with fewer samples

    def select_readable_ticks(values, limit):
        if not values:
            return []
        multiples_of_100 = [value for value in values if value % 100 == 0]
        if multiples_of_100:
            return multiples_of_100
        if len(values) <= limit:
            return values
        step = math.ceil(len(values) / limit)
        ticks = values[::step]
        if ticks[-1] != values[-1]:
            ticks.append(values[-1])
        return ticks

    def bin_and_average(df):
        if df.empty:
            return pd.DataFrame(columns=['bin_start', 'elapsed_ms_mean', 'elapsed_ms_std'])

        binned = df.copy()
        binned['tuples'] = pd.to_numeric(binned['tuples'], errors='coerce')
        binned['elapsed_ms'] = pd.to_numeric(binned['elapsed_ms'], errors='coerce')
        binned = binned.dropna(subset=['tuples', 'elapsed_ms'])
        binned['bin_start'] = (binned['tuples'] // bin_size) * bin_size
        result = (
            binned.groupby('bin_start', as_index=False)['elapsed_ms']
            .agg(elapsed_ms_mean='mean', elapsed_ms_std='std', count='size')
            .sort_values('bin_start')
        )
        # Filter out bins with fewer than min_bin_count samples
        return result[result['count'] >= min_bin_count].drop(columns=['count'])

    avg1 = bin_and_average(df1)
    avg2 = bin_and_average(df2)
    avg3 = bin_and_average(df3)
    avg4 = bin_and_average(df4)

    all_bins = sorted(
        set(avg1['bin_start'])
        .union(set(avg2['bin_start']))
        .union(set(avg3['bin_start']))
        .union(set(avg4['bin_start']))
    )

    plt.figure()
    plotter = plt.errorbar if show_error_bars else plt.plot
    if show_error_bars:
        plotter(avg1['bin_start'], avg1['elapsed_ms_mean'], yerr=avg1['elapsed_ms_std'].fillna(0), marker='o', label="static", capsize=3)
        plotter(avg2['bin_start'], avg2['elapsed_ms_mean'], yerr=avg2['elapsed_ms_std'].fillna(0), marker='o', label="always", capsize=3)
        plotter(avg3['bin_start'], avg3['elapsed_ms_mean'], yerr=avg3['elapsed_ms_std'].fillna(0), marker='o', label="on distribution change", capsize=3)
        plotter(avg4['bin_start'], avg4['elapsed_ms_mean'], yerr=avg4['elapsed_ms_std'].fillna(0), marker='o', label="on ranking change", capsize=3)
    else:
        plotter(avg1['bin_start'], avg1['elapsed_ms_mean'], marker='o', label="static")
        plotter(avg2['bin_start'], avg2['elapsed_ms_mean'], marker='o', label="always")
        plotter(avg3['bin_start'], avg3['elapsed_ms_mean'], marker='o', label="on distribution change")
        plotter(avg4['bin_start'], avg4['elapsed_ms_mean'], marker='o', label="on ranking change")
    plt.title(title)
    plt.legend()
    plt.xlabel("Window size (#tuples)")
    plt.ylabel("Average elapsed time (ms)")
    if all_bins:
        readable_bins = select_readable_ticks(all_bins, max_ticks)
        labels = [f"{int(start)}" for start in readable_bins]
        plt.xticks(readable_bins, labels, rotation=30, ha='right')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    if filename:
        os.makedirs("out", exist_ok=True)
        plt.savefig(filename, dpi=300, bbox_inches='tight')
    if SHOW_PLOTS:
        plt.show()
    else:
        plt.close()

def make_query_vs_optimize_comparison(title, data1, data2, data3, data4, filename=None):
    """
    Create separate figures comparing query time vs optimize time per window size for each method.
    """
    bin_size = 25
    max_ticks = 8
    min_bin_count = 50  # Filter out bins with fewer samples

    def select_readable_ticks(values, limit):
        if not values:
            return []
        multiples_of_100 = [value for value in values if value % 100 == 0]
        if multiples_of_100:
            return multiples_of_100
        if len(values) <= limit:
            return values
        step = math.ceil(len(values) / limit)
        ticks = values[::step]
        if ticks[-1] != values[-1]:
            ticks.append(values[-1])
        return ticks
    
    def bin_and_average_by_phase(df):
        """Bin data by window size and average elapsed_ms for both phases."""
        if df.empty:
            return pd.DataFrame(columns=['bin_start', 'phase', 'elapsed_ms'])
        
        binned = df.copy()
        binned['tuples'] = pd.to_numeric(binned['tuples'], errors='coerce')
        binned['elapsed_ms'] = pd.to_numeric(binned['elapsed_ms'], errors='coerce')
        binned = binned.dropna(subset=['tuples', 'elapsed_ms', 'phase'])
        binned['bin_start'] = (binned['tuples'] // bin_size) * bin_size
        
        result = binned.groupby(['bin_start', 'phase'], as_index=False)['elapsed_ms'].agg(elapsed_ms='mean', count='size')
        # Filter out bins with fewer than min_bin_count samples
        return result[result['count'] >= min_bin_count].drop(columns=['count'])
    
    # Load CSV files
    df1 = pd.read_csv(data1 + ".csv")
    df2 = pd.read_csv(data2 + ".csv")
    df3 = pd.read_csv(data3 + ".csv")
    df4 = pd.read_csv(data4 + ".csv")
    
    # Bin and average by phase
    binned1 = bin_and_average_by_phase(df1)
    binned2 = bin_and_average_by_phase(df2)
    binned3 = bin_and_average_by_phase(df3)
    binned4 = bin_and_average_by_phase(df4)
    
    methods = [
        ("Static", binned1),
        ("Always", binned2),
        ("On Distribution Change", binned3),
        ("On Ranking Change", binned4)
    ]

    if filename:
        os.makedirs("out", exist_ok=True)
        base_name = os.path.splitext(filename)[0]

    for method_name, binned_data in methods:
        optimize_data = binned_data[binned_data['phase'] == 'optimize'].sort_values('bin_start')
        query_data = binned_data[binned_data['phase'] == 'query'].sort_values('bin_start')

        plt.figure()
        if not optimize_data.empty:
            plt.plot(optimize_data['bin_start'], optimize_data['elapsed_ms'], marker='o', label='Optimize', linewidth=2)
        if not query_data.empty:
            plt.plot(query_data['bin_start'], query_data['elapsed_ms'], marker='s', label='Query', linewidth=2)

        x_values = sorted(set(optimize_data['bin_start']).union(set(query_data['bin_start'])))
        if x_values:
            readable_ticks = select_readable_ticks(x_values, max_ticks)
            plt.xticks(readable_ticks, [f"{int(v)}" for v in readable_ticks], rotation=30, ha='right')

        plt.title("Query time vs Trigger time per window")
        # plt.title(f"{title} - {method_name}", fontsize=13, fontweight='bold')
        plt.xlabel("Window Size (tuples)")
        plt.ylabel("Average Elapsed Time (ms)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        if filename:
            plt.savefig(f"{base_name}_{method_name.replace(' ', '_').lower()}.png", dpi=300, bbox_inches='tight')

        if SHOW_PLOTS:
            plt.show()
        else:
            plt.close()

def make_phase_average_comparison(title, data1, data2, data3, data4, filename=None):
    """
    Create a grouped bar chart comparing average query vs optimize time for all four approaches.
    """
    def get_phase_averages(data_file):
        try:
            df = pd.read_csv(data_file + ".csv")
        except FileNotFoundError:
            return {"optimize": float("nan"), "query": float("nan")}

        if df.empty or 'phase' not in df.columns or 'elapsed_ms' not in df.columns:
            return {"optimize": float("nan"), "query": float("nan")}

        phase_df = df.copy()
        phase_df['elapsed_ms'] = pd.to_numeric(phase_df['elapsed_ms'], errors='coerce')
        phase_df = phase_df.dropna(subset=['phase', 'elapsed_ms'])

        averages = phase_df.groupby('phase', as_index=True)['elapsed_ms'].mean()
        return {
            'optimize': averages.get('optimize', float('nan')),
            'query': averages.get('query', float('nan')),
        }

    methods = [
        ("Static", get_phase_averages(data1)),
        ("Always", get_phase_averages(data2)),
        ("On Distribution Change", get_phase_averages(data3)),
        ("On Ranking Change", get_phase_averages(data4)),
    ]

    labels = [name for name, _ in methods]
    optimize_values = [0 if pd.isna(stats['optimize']) else stats['optimize'] for _, stats in methods]
    query_values = [0 if pd.isna(stats['query']) else stats['query'] for _, stats in methods]

    x_positions = range(len(labels))
    bar_width = 0.6

    plt.figure(figsize=(12, 6))
    plt.bar(list(x_positions), optimize_values, width=bar_width, label='Optimize')
    plt.bar(list(x_positions), query_values, width=bar_width, bottom=optimize_values, label='Query')

    plt.xticks(list(x_positions), labels, rotation=20, ha='right')
    plt.ylabel('Average elapsed time (ms)')
    plt.title(title)
    plt.legend()
    plt.grid(True, axis='y', alpha=0.3)
    plt.tight_layout()

    if filename:
        os.makedirs("out", exist_ok=True)
        plt.savefig(filename, dpi=300, bbox_inches='tight')
    if SHOW_PLOTS:
        plt.show()
    else:
        plt.close()

def make_avg_plot_by_threshold(title, data1, data2, data3, data4, filename=None):
    """
    Create a single figure showing query execution time vs threshold value.
    Each method is represented as a separate line.
    """
    
    def get_query_by_threshold(data_file):
        """Load CSV and group query phase data by threshold."""
        try:
            df = pd.read_csv(data_file + ".csv")
        except FileNotFoundError:
            return pd.DataFrame(columns=['threshold', 'elapsed_ms'])
        
        if df.empty:
            return pd.DataFrame(columns=['threshold', 'elapsed_ms'])
        
        # Filter for query phase only
        query_df = df[df['phase'] == 'query'].copy()
        if query_df.empty:
            return pd.DataFrame(columns=['threshold', 'elapsed_ms'])
        
        # Convert to numeric
        query_df['threshold'] = pd.to_numeric(query_df['threshold'], errors='coerce')
        query_df['elapsed_ms'] = pd.to_numeric(query_df['elapsed_ms'], errors='coerce')
        query_df = query_df.dropna(subset=['threshold', 'elapsed_ms'])
        
        # Group by threshold and average
        return query_df.groupby('threshold', as_index=False)['elapsed_ms'].mean().sort_values('threshold')
    
    # Load and process data for all methods
    data_static = get_query_by_threshold(data1)
    data_always = get_query_by_threshold(data2)
    data_dist = get_query_by_threshold(data3)
    data_rank = get_query_by_threshold(data4)
    
    # Create single plot with all methods
    plt.figure(figsize=(10, 6))
    
    # Plot each method as a line
    # if not data_static.empty:
    #     thresholds_pct = data_static['threshold'] * 100
    #     plt.plot(thresholds_pct, data_static['elapsed_ms'], 
    #             marker='o', label='Static', linewidth=2)
    
    # if not data_always.empty:
    #     thresholds_pct = data_always['threshold'] * 100
    #     plt.plot(thresholds_pct, data_always['elapsed_ms'], 
    #             marker='s', label='Always', linewidth=2)

    if not data_dist.empty:
        thresholds_pct = data_dist['threshold']
        plt.plot(thresholds_pct, data_dist['elapsed_ms'], 
                marker='^', label='On Distribution Change', linewidth=2)
    
    if not data_rank.empty:
        thresholds_pct = data_rank['threshold']
        print(max(data_rank['elapsed_ms']))
        plt.plot(thresholds_pct, data_rank['elapsed_ms'], 
                marker='d', label='On Ranking Change', linewidth=2)

    plt.title(title, fontsize=14, fontweight='bold')
    plt.xlabel("Threshold", fontsize=12)
    plt.ylabel("Average Query Execution Time (ms)", fontsize=12)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if filename:
        os.makedirs("out", exist_ok=True)
        plt.savefig(filename, dpi=300, bbox_inches='tight')
    if SHOW_PLOTS:
        plt.show()
    else:
        plt.close()

def split_data(title, data1, data2, data3, data4, filename1=None, filename2=None, show_error_bars=False):
    # Load CSV file
    df1 = pd.read_csv(data1 + ".csv")
    df2 = pd.read_csv(data2 + ".csv")
    df3 = pd.read_csv(data3 + ".csv")
    df4 = pd.read_csv(data4 + ".csv")

    # Preview data
    # print(df1.head())
    # print(df2.head())
    # print(df3.head())
    # print(df4.head())

    # Split data by phase
    df1_opt = df1[df1['phase'] == 'optimize']
    df1_query = df1[df1['phase'] == 'query']
    df2_opt = df2[df2['phase'] == 'optimize']
    df2_query = df2[df2['phase'] == 'query']
    df3_opt = df3[df3['phase'] == 'optimize']
    df3_query = df3[df3['phase'] == 'query']
    df4_opt = df4[df4['phase'] == 'optimize']
    df4_query = df4[df4['phase'] == 'query']

    make_avg_plot_by_window_size(
        f"{title} - Optimize Phase (Average elapsed_ms per window size)",
        df1_opt,
        df2_opt,
        df3_opt,
        df4_opt,
        f"out/{filename1}.png",
        show_error_bars=show_error_bars,
    )
    make_avg_plot_by_window_size(
        f"Query phase - {title} datastream - Save stats on trigger",
        df1_query,
        df2_query,
        df3_query,
        df4_query,
        f"out/{filename2}.png",
        show_error_bars=show_error_bars,
    )

def main():
    static1 = "target/experiment_logs/optimizer_case_static.events_Static"
    static2 = "target/experiment_logs/optimizer_case_static.events_Always"
    static3 = "target/experiment_logs/optimizer_case_static.events_OnDistributionChange"
    static4 = "target/experiment_logs/optimizer_case_static.events_OnRankingChange"

    volatile1 = "target/experiment_logs/optimizer_case_volatile.events_Static"
    volatile2 = "target/experiment_logs/optimizer_case_volatile.events_Always"
    volatile3 = "target/experiment_logs/optimizer_case_volatile.events_OnDistributionChange"
    volatile4 = "target/experiment_logs/optimizer_case_volatile.events_OnRankingChange"

    gradual1 = "target/experiment_logs/optimizer_case_gradual.events_Static"
    gradual2 = "target/experiment_logs/optimizer_case_gradual.events_Always"
    gradual3 = "target/experiment_logs/optimizer_case_gradual.events_OnDistributionChange"
    gradual4 = "target/experiment_logs/optimizer_case_gradual.events_OnRankingChange"

    extra = ""

    global SHOW_PLOTS
    parser = argparse.ArgumentParser()
    parser.add_argument('--show', action='store_true', help='Show plots interactively instead of saving and closing')
    args, unknown = parser.parse_known_args()
    SHOW_PLOTS = bool(args.show)

    # Original separate phase plots
    split_data("Static", static1, static2, static3, static4, f"static_optimize{extra}", f"static_execution{extra}", show_error_bars=False)
    split_data("Volatile", volatile1, volatile2, volatile3, volatile4, f"volatile_optimize{extra}", f"volatile_execution{extra}", show_error_bars=False)
    split_data("Gradual", gradual1, gradual2, gradual3, gradual4, f"gradual_optimize{extra}", f"gradual_execution{extra}", show_error_bars=False)
    
    # New query vs optimize comparison plots
    make_query_vs_optimize_comparison(
        "Static Dataset: Query vs Optimize Time per Window Size",
        static1, static2, static3, static4,
        f"out/static_query_vs_optimize{extra}.png"
    )
    make_phase_average_comparison(
        "Static Dataset: Average Query vs Optimize Time",
        static1, static2, static3, static4,
        f"out/static_phase_average{extra}.png"
    )
    make_query_vs_optimize_comparison(
        "Volatile Dataset: Query vs Optimize Time per Window Size",
        volatile1, volatile2, volatile3, volatile4,
        f"out/volatile_query_vs_optimize{extra}.png"
    )
    make_phase_average_comparison(
        "Volatile Dataset: Average Query vs Optimize Time",
        volatile1, volatile2, volatile3, volatile4,
        f"out/volatile_phase_average{extra}.png"
    )
    make_query_vs_optimize_comparison(
        "Gradual Dataset: Query vs Optimize Time per Window Size",
        gradual1, gradual2, gradual3, gradual4,
        f"out/gradual_query_vs_optimize{extra}.png"
    )
    make_phase_average_comparison(
        "Gradual Dataset: Average Query vs Optimize Time",
        gradual1, gradual2, gradual3, gradual4,
        f"out/gradual_phase_average{extra}.png"
    )
    
    # # New query execution time vs threshold plots
    # make_avg_plot_by_threshold(
    #     "Static Dataset: Query Execution Time per Threshold",
    #     static1, static2, static3, static4,
    #     f"out/static_query_vs_threshold{extra}.png"
    # )
    # make_avg_plot_by_threshold(
    #     "Volatile Dataset: Query Execution Time per Threshold",
    #     volatile1, volatile2, volatile3, volatile4,
    #     f"out/volatile_query_vs_threshold{extra}.png"
    # )
    # make_avg_plot_by_threshold(
    #     "Gradual Dataset: Query Execution Time per Threshold",
    #     gradual1, gradual2, gradual3, gradual4,
    #     f"out/gradual_query_vs_threshold{extra}.png"
    # )

if __name__ == "__main__":
    main()