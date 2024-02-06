from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

references = map(Path, [
    "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical.csv",

    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical-1.9.3.csv"
])

statistics = map(Path, [
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_11.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_12.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_13.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_14.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_15.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_16.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_17.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_18.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_19.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_20.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_21.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_22.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_23.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_24.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_25.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_26.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_VBS.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_24.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_25.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_26.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_27.0.csv"
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_34.0.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_24.0.csv"
])

def parse_table(reference_df, exp_name):
    reference_df['task_name'] = reference_df['file_name'].str.replace(f'{exp_name}_', '', 1)
    # Формирование новой таблицы с колонками 'time' и 'task_name'
    output_df = reference_df[['time', 'task_name']]

    output_df = output_df.rename(columns={'time': f'{exp_name}'})

    return output_df[[f'{exp_name}', 'task_name']]

def join_and_check_duplicates(reference_name, df1, experiments_name, df2, on_column):
    merged_df = pd.merge(df1, df2, on=on_column, how='inner', suffixes=(f'_{reference_name}', f'_{experiments_name}'))

    duplicates = merged_df[merged_df.duplicated(subset=on_column, keep=False)]

    if not duplicates.empty:
        raise ValueError(f"Duplicates found in the merged DataFrame: {duplicates}")

    return merged_df

for reference in references:
    ref_name = reference.stem
    ref_df = parse_table(pd.read_csv(reference), ref_name)
    for exp in statistics:
        exp_name = exp.stem
        exp_df = parse_table(pd.read_csv(exp), exp_name)
        scatter_df = join_and_check_duplicates(ref_name, ref_df, exp_name, exp_df, "task_name")
        df = scatter_df.drop(['task_name'], axis=1)
        first, second = df.columns
        # Генерация scatter plot с логарифмической шкалой по осям
        inch = (1.65, 1.65)
        plt.figure(figsize=inch, dpi=300)
        plt.scatter(df[first], df[second], s=3, zorder=2)

        # Добавление диагональной линии
        min_val = min(df[first].min(), df[second].min())
        max_val = max(df[first].max(), df[second].max())
        plt.plot([min_val, max_val], [min_val, max_val], '--', color='gray',
                 markersize=0.1, linewidth=1, dashes=(2, 2), zorder=1)


        plt.xlabel(f'Baseline time (CaDiCaL 1.9.3), s', fontsize=5.8)
        if 'VBS' in second:
            # plt.title(f'{second} configuration', fontsize=8)
            plt.ylabel(f"Time ({second}), s", fontsize=5.8)
        else:
            # plt.title(f'{second[:-2]} configuration', fontsize=8)
            plt.ylabel(f"Time ({second[:-2]}), s", fontsize=5.8)

        plt.tick_params(axis='both', which='both', labelsize=3.5)
        plt.grid(True, linewidth=0.2)
        plt.subplots_adjust(left=0.22, bottom=0.20, right=0.99, top=0.90)
        # fig = plt.gcf()
        # fig.set_size_inches(3.3, fig.get_figheight())


        # Сохранение графика в файл PNG
        plt.savefig(f'{second}_{inch[0]}_{inch[1]}.pdf', format='pdf')

        # Отображение графика
        plt.show()
