import pandas as pd
import matplotlib.pyplot as plt

def make_plot(title, df1, df2, df3, df4, filename=None):
    plt.plot(df1['ts'], df1['elapsed_ms'], marker='o', label="static")
    plt.plot(df2['ts'], df2['elapsed_ms'], marker='o', label="always")
    plt.plot(df3['ts'], df3['elapsed_ms'], marker='o', label="on distribution change")
    plt.plot(df4['ts'], df4['elapsed_ms'], marker='o', label="on ranking change")
    plt.title(title)
    plt.legend()
    plt.xlabel("Triple timestamp")
    plt.ylabel("Elapsed time (ms)")
    if filename:
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

    make_plot(f"{title} - Optimize Phase", df1_opt, df2_opt, df3_opt, df4_opt, f"out/{filename1}.png")
    make_plot(f"{title} - Execution Phase", df1_query, df2_query, df3_query, df4_query, f"out/{filename2}.png")

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

    split_data("Static", static1, static2, static3, static4, f"static_optimize{extra}", f"static_execution{extra}")
    split_data("Volatile", volatile1, volatile2, volatile3, volatile4, f"volatile_optimize{extra}", f"volatile_execution{extra}")
    split_data("Gradual", gradual1, gradual2, gradual3, gradual4, f"gradual_optimize{extra}", f"gradual_execution{extra}")

if __name__ == "__main__":
    main()