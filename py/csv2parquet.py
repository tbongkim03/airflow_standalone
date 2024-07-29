import pandas as pd
import sys

def csv2parquet():
    READ_PATH = sys.argv[1]
    SAVE_PATH = sys.argv[2]
    df = pd.read_csv(READ_PATH, on_bad_lines='skip', encoding='latin', names=['dt', 'cmd', 'cnt'])
    df['dt'] = df['dt'].str.replace('^', '')
    df['cmd'] = df['cmd'].str.replace('^', '')
    df['cnt'] = df['cnt'].str.replace('^', '')

    # coerce는 변환할 수 없는 데이터를 만나면 그 값을 강제로 NaN으로 바꿈
    df['cnt'] = pd.to_numeric(df['cnt'], errors='coerce')
    # NaN 값을 원하는 방식으로 처리합니다. 0으로 예
    df['cnt'] =df['cnt'].fillna(0).astype(int)
    #df['cnt'] = df['cnt'].astype(int)

    df.to_parquet(f'{SAVE_PATH}', partition_cols=['dt'])

csv2parquet()
