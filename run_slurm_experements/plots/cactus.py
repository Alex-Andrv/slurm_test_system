from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

references = map(Path, [
    "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/sorts_4.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical-1.9.3.csv",
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
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_27.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_29.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_30.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_31.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_32.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_33.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_34.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_35.0.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_34.0.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_24.0.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_26.0.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_25.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical.csv"
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/sorts_5.0.csv",
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/sorts_6.0.csv",
])

def parse_table(reference_df, exp_name):
    reference_df['file_name'] = reference_df['file_name'].str.replace(f'{exp_name}_', '', 1)
    # Формирование новой таблицы с колонками 'time' и 'file_name'
    output_df = reference_df[['time', 'file_name']]

    output_df = output_df.rename(columns={'time': f'{exp_name}'})

    return output_df[[f'{exp_name}', 'file_name']]

def join_and_check_duplicates(reference_name, df1, experiments_name, df2, on_column):
    merged_df = pd.merge(df1, df2, on=on_column, how='inner')

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
        ref_df = join_and_check_duplicates(ref_name, ref_df, exp_name, exp_df, "file_name")


    column_order = ['file_name'] + [col for col in ref_df.columns if col != 'file_name']
    portfolio = [col for col in ref_df.columns if (col != 'file_name') and ("cadical" not in col)]
    portfolio = [col for col in ref_df.columns if (col != 'file_name')]
    portfolio_with_cadical = [col for col in ref_df.columns if (col != 'file_name')]
    df = ref_df[column_order]
    # df.loc[:, 'VBS'] = df[portfolio].min(axis=1)
    df['interleave_VBS'] = df[portfolio].min(axis=1)
    # df['VBS'] = df[portfolio_with_cadical].min(axis=1)
    df.to_csv('ref_df.csv', index=False)
    df[['file_name', 'interleave_VBS']].to_csv('interleave_VBS.csv', index=False)
