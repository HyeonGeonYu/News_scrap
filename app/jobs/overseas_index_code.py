'''
* 해외주식지수정보(frgn_code.mst) 정제 파이썬 파일
* 정제완료된 엑셀파일 : frgn_code.xlsx
* overseas_index_code.py(frgn_code.mst)은 해외지수 정보 제공용으로 개발된 파일로
  해외주식 정보에 대해 얻고자 할 경우 overseas_stock_code.py(ex. nasmst.cod) 이용하시기 바랍니다.
'''
import json
import urllib.request
import os
import pandas as pd
import zipfile
import io
import urllib.request
from app.redis_client import redis_client
base_dir = os.getcwd()
def save_overseas_index_master_to_redis(redis_file_key):
    url = "https://new.real.download.dws.co.kr/common/master/frgn_code.mst.zip"

    # URL에서 zip 파일을 메모리로 읽기
    with urllib.request.urlopen(url) as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_file:
            # frgn_code.mst 파일 읽기
            with zip_file.open('frgn_code.mst') as mst_file:
                lines = mst_file.read().decode('cp949').splitlines()

    part1 = []
    part2 = []

    for row in lines:
        if row[0:1] == 'X':
            rf1 = row[0:len(row) - 14]
            rf_1 = rf1[0:1]
            rf1_2 = rf1[1:11]
            rf1_3 = rf1[11:40].replace(",", "")
            rf1_4 = rf1[40:80].replace(",", "").strip()
            part1.append([rf_1, rf1_2, rf1_3, rf1_4])
            rf2 = row[-15:]
            part2.append(rf2)
        else:
            rf1 = row[0:len(row) - 14]
            rf1_1 = rf1[0:1]
            rf1_2 = rf1[1:11]
            rf1_3 = rf1[11:50].replace(",", "")
            rf1_4 = row[50:75].replace(",", "").strip()
            part1.append([rf1_1, rf1_2, rf1_3, rf1_4])
            rf2 = row[-15:]
            part2.append(rf2)

    # 데이터프레임 생성
    df1 = pd.DataFrame(part1, columns=['구분코드', '심볼', '영문명', '한글명'])

    # df2는 고정폭 파일처럼 처리
    field_specs = [4, 1, 1, 1, 4, 3]
    df2 = pd.DataFrame([[
        r[:4], r[4:5], r[5:6], r[6:7], r[7:11], r[11:14]
    ] for r in part2], columns=[
        '종목업종코드', '다우30 편입종목여부', '나스닥100 편입종목여부',
        'S&P 500 편입종목여부', '거래소코드', '국가구분코드'
    ])

    # 정제
    df2['종목업종코드'] = df2['종목업종코드'].str.replace(r'[^A-Z]', '', regex=True)
    df2['다우30 편입종목여부'] = df2['다우30 편입종목여부'].str.replace(r'[^0-1]', '', regex=True)
    df2['나스닥100 편입종목여부'] = df2['나스닥100 편입종목여부'].str.replace(r'[^0-1]', '', regex=True)
    df2['S&P 500 편입종목여부'] = df2['S&P 500 편입종목여부'].str.replace(r'[^0-1]', '', regex=True)

    df = pd.concat([df1, df2], axis=1)
    # Redis에 저장 (json 문자열로)
    redis_client.set(redis_file_key, df.to_json(orient='records', force_ascii=False))
    return df

# Redis에서 데이터를 가져오는 함수
def get_overseas_index_master_from_redis(redis_file_key):
    # Redis에서 KIS 키로 저장된 데이터를 가져옴
    kis_data = redis_client.get(redis_file_key)
    if kis_data is not None:
        # Redis에서 가져온 데이터를 JSON으로 디코딩한 후 DataFrame으로 변환
        kis_data_json = json.loads(kis_data.decode('utf-8'))  # 'bytes' 데이터를 'utf-8'로 디코딩
        kis_df = pd.DataFrame(kis_data_json)  # DataFrame으로 변환
        return kis_df
    else:
        print(f"No data found in Redis for key: {redis_file_key}")
        return None
if __name__ == "__main__":
    df = save_overseas_index_master_to_redis("KIS_file:frgn_code")
    print("Done")
    df = get_overseas_index_master_from_redis("KIS_file:frgn_code")
    if df is not None:
        print(df.head())
    else:
        print("Data not found in Redis.")
