import pandas as pd
import matplotlib.pyplot as plt
import os

def make_avg_plot_by_window_size(title, df1, df2, df3, df4, filename=None):
    bin_size = 25

    def bin_and_average(df):
        if df.empty:
            return pd.DataFrame(columns=['bin_start', 'elapsed_ms'])

        binned = df.copy()
        binned['tuples'] = pd.to_numeric(binned['tuples'], errors='coerce')
        binned['elapsed_ms'] = pd.to_numeric(binned['elapsed_ms'], errors='coerce')
        binned = binned.dropna(subset=['tuples', 'elapsed_ms'])
        binned['bin_start'] = (binned['tuples'] // bin_size) * bin_size
        return (
            binned.groupby('bin_start', as_index=False)['elapsed_ms']
            .mean()
            .sort_values('bin_start')
        )

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
    plt.plot(avg1['bin_start'], avg1['elapsed_ms'], marker='o', label="static")
    plt.plot(avg2['bin_start'], avg2['elapsed_ms'], marker='o', label="always")
    plt.plot(avg3['bin_start'], avg3['elapsed_ms'], marker='o', label="on distribution change")
    plt.plot(avg4['bin_start'], avg4['elapsed_ms'], marker='o', label="on ranking change")
    plt.title(title)
    plt.legend()
    plt.xlabel("Window size")
    plt.ylabel("Average elapsed time (ms)")
    if all_bins:
        labels = [f"{int(start)}" for start in all_bins]
        plt.xticks(all_bins, labels, rotation=45)
    plt.tight_layout()
    if filename:
        os.makedirs("out", exist_ok=True)
        plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()

def make_query_vs_optimize_comparison(title, data1, data2, data3, data4, filename=None):
    """
    Create a single figure comparing query time vs optimize time per window size for all methods.
    Each method gets its own subplot showing both phases.
    """
    bin_size = 25
    
    def bin_and_average_by_phase(df):
        """Bin data by window size and average elapsed_ms for both phases."""
        if df.empty:
            return pd.DataFrame(columns=['bin_start', 'phase', 'elapsed_ms'])
        
        binned = df.copy()
        binned['tuples'] = pd.to_numeric(binned['tuples'], errors='coerce')
        binned['elapsed_ms'] = pd.to_numeric(binned['elapsed_ms'], errors='coerce')
        binned = binned.dropna(subset=['tuples', 'elapsed_ms', 'phase'])
        binned['bin_start'] = (binned['tuples'] // bin_size) * bin_size
        
        return binned.groupby(['bin_start', 'phase'], as_index=False)['elapsed_ms'].mean()
    
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
    
    # Create 2x2 subplot layout for 4 methods
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()
    
    methods = [
        ("Static", binned1),
        ("Always", binned2),
        ("On Distribution Change", binned3),
        ("On Ranking Change", binned4)
    ]
    
    for idx, (method_name, binned_data) in enumerate(methods):
        ax = axes[idx]
        
        # Separate optimize and query phases
        optimize_data = binned_data[binned_data['phase'] == 'optimize'].sort_values('bin_start')
        query_data = binned_data[binned_data['phase'] == 'query'].sort_values('bin_start')
        
        # Plot both phases
        if not optimize_data.empty:
            ax.plot(optimize_data['bin_start'], optimize_data['elapsed_ms'], 
                   marker='o', label='Optimize', linewidth=2)
        if not query_data.empty:
            ax.plot(query_data['bin_start'], query_data['elapsed_ms'], 
                   marker='s', label='Query', linewidth=2)
        
        ax.set_title(f"{method_name}", fontsize=12, fontweight='bold')
        ax.set_xlabel("Window Size (tuples)")
        ax.set_ylabel("Average Elapsed Time (ms)")
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    fig.suptitle(title, fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    if filename:
        os.makedirs("out", exist_ok=True)
        plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()

def split_data(title, data1, data2, data3, data4, filename1=None, filename2=None):
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
    )
    make_avg_plot_by_window_size(
        f"{title} - Query Phase (Average elapsed_ms per window size)",
        df1_query,
        df2_query,
        df3_query,
        df4_query,
        f"out/{filename2}.png",
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

    # Original separate phase plots
    split_data("Static", static1, static2, static3, static4, f"static_optimize{extra}", f"static_execution{extra}")
    split_data("Volatile", volatile1, volatile2, volatile3, volatile4, f"volatile_optimize{extra}", f"volatile_execution{extra}")
    split_data("Gradual", gradual1, gradual2, gradual3, gradual4, f"gradual_optimize{extra}", f"gradual_execution{extra}")
    
    # New query vs optimize comparison plots
    make_query_vs_optimize_comparison(
        "Static Dataset: Query vs Optimize Time per Window Size",
        static1, static2, static3, static4,
        f"out/static_query_vs_optimize{extra}.png"
    )
    make_query_vs_optimize_comparison(
        "Volatile Dataset: Query vs Optimize Time per Window Size",
        volatile1, volatile2, volatile3, volatile4,
        f"out/volatile_query_vs_optimize{extra}.png"
    )
    make_query_vs_optimize_comparison(
        "Gradual Dataset: Query vs Optimize Time per Window Size",
        gradual1, gradual2, gradual3, gradual4,
        f"out/gradual_query_vs_optimize{extra}.png"
    )

if __name__ == "__main__":
    main()