from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

references = map(Path, [
    # "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical.csv",
    "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical-satcomp2023-1.9.5.csv",

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
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/sbva_cadical-1.9.3.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/sbva_interleave_24.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_34.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_24.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_26.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_25.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_36.0.csv"
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical.csv"
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/sorts_5.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/sorts_6.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_36.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_37.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_38.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_39.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_40.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_41.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_24.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_42.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_36.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_37.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_38.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_39.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_40.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_41.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_42.0.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_103.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_102.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_2023_104.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_2023_105.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_106.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_107.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_108.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_109.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_109a.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_110.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_111.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_112.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_113.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_114.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_115.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_116.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_117.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_118.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_119.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_120.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_121.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_122.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_123.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_124.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_125.csv",
"/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave_v2_2023_126.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_43.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_24.0.csv",
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/cadical-satcomp2023-1.9.5.csv",
#     "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/interleave2023_43.0.csv"
# "/nfs/home/aandreev/slurm_test_system/run_slurm_experements/statistics/kissat-satcomp2023-3.1.1.csv",
])

def parse_table(reference_df, exp_name):
    reference_df['file_name'] = reference_df['file_name'].str.replace(f'{exp_name}_', '', 1)
    # Формирование новой таблицы с колонками 'time' и 'file_name'
    output_df = reference_df[['time', 'file_name', 'status']]

    output_df = output_df.rename(columns={'time': f'{exp_name}'})

    if "cadical" in exp_name:
        output_df['status'] = output_df['status'].replace({'UNDEFINED': 'UNDEF', 'UNSATISFIABLE': 'UNSAT', 'SATISFIABLE': 'SAT'})
    elif "int" in exp_name:
        output_df['status'] = output_df['status'].replace({'SLURM_TIME_LIMIT': 'UNDEF', 'UNDEFINED/UNSAT': 'UNSAT', 'SAT': 'SAT'})
    else:
        raise ValueError(exp_name)

    return output_df[[f'{exp_name}', 'file_name', 'status']]

def join_and_check_duplicates(reference_name, df1, experiments_name, df2, on_column):
    df2 = df2.rename(columns={'status': f'status_y'})
    merged_df = pd.merge(df1, df2, on=on_column, how='inner')
    print(experiments_name)
    print(merged_df[~((merged_df['status'].values == merged_df['status_y'].values) | (merged_df['status'].values == "UNDEF") | (merged_df['status_y'].values == "UNDEF") | (merged_df['status_y'].values == "OUT_OF_MEMORY"))])
    assert ((merged_df['status'].values == merged_df['status_y'].values) | (merged_df['status'].values == "UNDEF") | (merged_df['status_y'].values == "UNDEF") | (merged_df['status_y'].values == "OUT_OF_MEMORY")).all() == True
    merged_df = merged_df.drop(columns=['status_y'])
    merged_df = merged_df.rename(columns={'time': f'{exp_name}'})
    duplicates = merged_df[merged_df.duplicated(subset=on_column, keep=False)]

    if not duplicates.empty:
        raise ValueError(f"Duplicates found in the merged DataFrame: {duplicates}")

    return merged_df

def map_value_to(name):
    if "cadical" in name:
        cadical, _,  version = name.split("-")
        name = f"{cadical}-{version}"
    elif "interleave_v2" in name:
        interleave, v, _, version = name.split("_")
        name = f"int-{version}"
    elif "interleave" in name:
        interleave, _, version = name.split("_")
        name = f"int-{version}"
    else:
        raise ValueError(f"Unexpected solver: {name}")
    return name

for reference in references:
    ref_name = reference.stem
    ref_df = parse_table(pd.read_csv(reference), ref_name)
    for exp in statistics:
        exp_name = exp.stem
        exp_df = parse_table(pd.read_csv(exp), exp_name)
        ref_df = join_and_check_duplicates(ref_name, ref_df, exp_name, exp_df, "file_name")

    ref_df = ref_df.drop(columns=['status'])
    column_order = ['file_name'] + [col for col in ref_df.columns if col != 'file_name']
    portfolio = [col for col in ref_df.columns if (col != 'file_name') and ("cadical" not in col)]
    portfolio_with_cadical = [col for col in ref_df.columns if (col != 'file_name')]
    # portfolio = [col for col in ref_df.columns if (col != 'file_name')]
    # portfolio_with_cadical = [col for col in ref_df.columns if (col != 'file_name')]
    df = ref_df[column_order]
    # df.loc[:, 'VBS'] = df[portfolio].min(axis=1)
    df['VBS-interleave'] = df[portfolio].min(axis=1)

    df['VBS'] = df[portfolio_with_cadical].min(axis=1)
    df = df.rename(columns={column: map_value_to(column)
                            for column in ref_df.columns if column not in set(['file_name', 'VBS'])})
    df.to_csv('ref_df.csv', index=False)
    # df[['file_name', 'interleave_VBS']].to_csv('interleave_VBS.csv', index=False)
