'''해외선물옵션종목코드 정제 파이썬 파일 : ffcode.mst'''

import json
import urllib.request
import ssl
import zipfile
import io
import os
import pandas as pd
from app.redis_client import redis_client

base_dir = os.getcwd()

def save_overseas_future_master_to_redis(redis_file_key):
    # 1. URL에서 ZIP 파일을 메모리로 다운로드
    ssl._create_default_https_context = ssl._create_unverified_context
    url = "https://new.real.download.dws.co.kr/common/master/ffcode.mst.zip"
    with urllib.request.urlopen(url) as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_ref:
            # 2. ZIP 파일에서 ffcode.mst 추출
            zip_ref.extract('ffcode.mst', base_dir)

    file_name = os.path.join(base_dir, "ffcode.mst")

    # 3. 데이터 처리 및 정제
    columns = ['종목코드', '서버자동주문 가능 종목 여부', '서버자동주문 TWAP 가능 종목 여부', '서버자동 경제지표 주문 가능 종목 여부',
               '필러', '종목한글명', '거래소코드 (ISAM KEY 1)', '품목코드 (ISAM KEY 2)', '품목종류', '출력 소수점', '계산 소수점',
               '틱사이즈', '틱가치', '계약크기', '가격표시진법', '환산승수', '최다월물여부 0:원월물 1:최다월물',
               '최근월물여부 0:원월물 1:최근월물', '스프레드여부', '스프레드기준종목 LEG1 여부', '서브 거래소 코드']
    df = pd.DataFrame(columns=columns)

    ridx = 1
    print("Downloading...")

    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            a = row[:32]  # 종목코드
            b = row[32:33].rstrip()  # 서버자동주문 가능 종목 여부
            c = row[33:34].rstrip()  # 서버자동주문 TWAP 가능 종목 여부
            d = row[34:35]  # 서버자동 경제지표 주문 가능 종목 여부
            e = row[35:82].rstrip()  # 필러
            f = row[82:107].rstrip()  # 종목한글명
            g = row[-92:-82]  # 거래소코드 (ISAM KEY 1)
            h = row[-82:-72].rstrip()  # 품목코드 (ISAM KEY 2)
            i = row[-72:-69].rstrip()  # 품목종류
            j = row[-69:-64]  # 출력 소수점
            k = row[-64:-59].rstrip()  # 계산 소수점
            l = row[-59:-45].rstrip()  # 틱사이즈
            m = row[-45:-31]  # 틱가치
            n = row[-31:-21].rstrip()  # 계약크기
            o = row[-21:-17].rstrip()  # 가격표시진법
            p = row[-17:-7]  # 환산승수
            q = row[-7:-6].rstrip()  # 최다월물여부 0:원월물 1:최다월물
            r = row[-6:-5].rstrip()  # 최근월물여부 0:원월물 1:최근월물
            s = row[-5:-4].rstrip()  # 스프레드여부
            t = row[-4:-3].rstrip()  # 스프레드기준종목 LEG1 여부 Y/N
            u = row[-3:].rstrip()  # 서브 거래소 코드

            df.loc[ridx] = [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u]
            ridx += 1

    # 4. Redis에 저장 (json 형식으로)
    redis_client.set(redis_file_key, df.to_json(orient='records', force_ascii=False))
    print("Data saved to Redis")

    # clean up extracted file
    os.remove(file_name)

    return df


# Redis에서 데이터를 가져오는 함수
def get_overseas_future_master_from_redis(redis_file_key):
    # Redis에서 KIS 키로 저장된 데이터를 가져옴
    ffcode_data = redis_client.get(redis_file_key)
    if ffcode_data is not None:
        # Redis에서 가져온 데이터를 JSON으로 디코딩한 후 DataFrame으로 변환
        ffcode_data_json = json.loads(ffcode_data.decode('utf-8'))  # 'bytes' 데이터를 'utf-8'로 디코딩
        ffcode_df = pd.DataFrame(ffcode_data_json)  # DataFrame으로 변환
        return ffcode_df
    else:
        print(f"No data found in Redis for key: {redis_file_key}")
        return None


if __name__ == "__main__":
    # 1. Redis에 데이터 저장
    df = save_overseas_future_master_to_redis("KIS_file:ffcode")
    print("Done saving to Redis")

    # 2. Redis에서 데이터 가져오기
    df = get_overseas_future_master_from_redis("KIS_file:ffcode")
    if df is not None:
        print(df.head())
    else:
        print("Data not found in Redis.")