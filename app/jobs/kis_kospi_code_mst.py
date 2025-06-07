'''코스피주식종목코드(kospi_code.mst) 정제 파이썬 파일'''

import urllib.request
import ssl
import zipfile
import os
import pandas as pd
import io
from app.redis_client import redis_client
import json
base_dir = os.getcwd()

def save_kospi_master_to_redis(redis_file_key):
    # 1. URL에서 ZIP 파일을 메모리로 다운로드
    ssl._create_default_https_context = ssl._create_unverified_context
    url = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
    with urllib.request.urlopen(url) as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_ref:
            with zip_ref.open('kospi_code.mst') as file:
                # 3. 데이터 처리 및 정제 (임시 파일 사용 안함)
                part1_data = []
                part2_data = []

                for row in file:
                    row = row.decode('cp949')  # 한글 인코딩을 위해 디코드
                    # 첫 번째 부분 (csv 형식으로 처리)
                    rf1 = row[0:len(row) - 228]
                    rf1_1 = rf1[0:9].rstrip()
                    rf1_2 = rf1[9:21].rstrip()
                    rf1_3 = rf1[21:].strip()
                    part1_data.append([rf1_1, rf1_2, rf1_3])

                    # 두 번째 부분 (고정 폭 형식으로 처리)
                    rf2 = row[-228:]
                    part2_data.append(rf2)


                # 4. DataFrame 생성
                part1_columns = ['단축코드', '표준코드', '한글명']
                df1 = pd.DataFrame(part1_data, columns=part1_columns)

                field_specs = [2, 1, 4, 4, 4,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1,
                               1, 9, 5, 5, 1,
                               1, 1, 2, 1, 1,
                               1, 2, 2, 2, 3,
                               1, 3, 12, 12, 8,
                               15, 21, 2, 7, 1,
                               1, 1, 1, 1, 9,
                               9, 9, 5, 9, 8,
                               9, 3, 1, 1, 1
                               ]
                part2_columns = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
                                 '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
                                 'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
                                 'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
                                 'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
                                 'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
                                 'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
                                 '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
                                 '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
                                 '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
                                 '상장주수', '자본금', '결산월', '공모가', '우선주',
                                 '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
                                 '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
                                 '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
                                 ]
                part2_data_split = []
                for row in part2_data:
                    row_split = []
                    start_index = 0
                    for width in field_specs:
                        row_split.append(row[start_index:start_index + width].strip())
                        start_index += width
                    part2_data_split.append(row_split)
                df2 = pd.DataFrame(part2_data_split, columns=part2_columns)
                df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

                # 5. Redis에 저장 (json 형식으로)
                redis_client.set(redis_file_key, df.to_json(orient='records', force_ascii=False))
                print("Data saved to Redis")
                return df
# Redis에서 데이터를 가져오는 함수
def get_kospi_master_from_redis(redis_file_key):
    # Redis에서 KIS 키로 저장된 데이터를 가져옴
    kospi_data = redis_client.get(redis_file_key)
    if kospi_data is not None:
        # Redis에서 가져온 데이터를 JSON으로 디코딩한 후 DataFrame으로 변환
        kospi_data_json = json.loads(kospi_data.decode('utf-8'))  # 'bytes' 데이터를 'utf-8'로 디코딩
        kospi_df = pd.DataFrame(kospi_data_json)  # DataFrame으로 변환
        return kospi_df
    else:
        print(f"No data found in Redis for key: {redis_file_key}")
        return None

if __name__ == "__main__":
    # 1. Redis에 데이터 저장
    df = save_kospi_master_to_redis("KIS_file:kospi_code")
    print("Done saving to Redis")

    # 2. Redis에서 데이터 가져오기
    df = get_kospi_master_from_redis("KIS_file:kospi_code")
    if df is not None:
        print(df.head())
    else:
        print("Data not found in Redis.")