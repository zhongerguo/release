# 第一步：导入警告处理模块（放在所有导入的最前面）
import warnings

from urllib3.exceptions import InsecureRequestWarning

# 第二步：过滤掉 "InsecureRequestWarning" 警告
warnings.filterwarnings("ignore", category=InsecureRequestWarning)

import threading
from time import sleep
from random import uniform,choice
from math import ceil
from httpx import Client
from requests.exceptions import RequestException
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0"
        # 可继续添加更多User-Agent
]

def init_client():
    client = Client(
        http2=True,
        verify=False,
        headers = {
            'User-Agent': choice(USER_AGENTS),
            'Referer': 'https://quote.eastmoney.com/',
            'Accept-Encoding': choice(['gzip','deflate','br','zstd']),
            'Cache-Control': 'no-cache',
            # 'Accept-Language': 'zh-TW,zh-CN;q=0.9,zh;q=0.8,en;q=0.7,en-GB;q=0.6,en-US;q=0.5',
            'Connection': 'keep-alive',
        }
    )
    return client

GLOBAL_SESSION = init_client()

def stock_page_get():
    headers = {
        'User-Agent': choice(USER_AGENTS),
    }

    url = 'https://push2.eastmoney.com/api/qt/clist/get/'

    param = {
        'np': '1',
        'fltt': '1',
        'invt': '2',
        'fs': 'm:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2',
        'pn': '1',
        'pz': '20',
        'po': '1',
        'webp2u': '|0|0|0|web'
    }

    try:
        while True:
            resp = GLOBAL_SESSION.get(url=url, params=param, headers=headers,timeout=15)
            resp.raise_for_status()
            # print(res)
            rrr = resp.json()
            total_page = rrr.get('data',{}).get('total')
            if total_page:
                return ceil(min(total_page/20,300))
            uniform(0.3,0.7)
    except Exception:
        return None

# url = 'https://datacenter.eastmoney.com/stock/selection/api/data/get/
def stock_craw_code(page):
    headers = {
        'User-Agent': choice(USER_AGENTS),
    }

    url = 'https://push2.eastmoney.com/api/qt/clist/get/'

    # secid 股票id
    param = {
        'np':'1',
        'fltt':'1',
        'invt':'2',
        'fs':'m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2',
        'fields':'f12,f13',
        'fid':'f3',
        'pn':page,
        'pz':'20',
        'po':'1',
        'webp2u':'|0|0|0|web'
    }
    try:
        try:
            resp = GLOBAL_SESSION.get(url=url, params=param, headers=headers,timeout=15)
            resp.raise_for_status()
        except RequestException:
            return (1,[])
        try:
            #print(res)
            rrr = resp.json()
            codes = rrr.get('data',{}).get('diff',[])
        except (json.decoder.JSONDecodeError, KeyError):
            return (2,[])
            #print(codes)
        if not(codes):
            return (3,[])

        stock_code_list = []
        for code in codes:
            code_str = code.get('f12')
            market_code_str = code.get('f13')
            if code_str and market_code_str in (1,0):
                stock_code_list.append({'code':code_str,'market':market_code_str})

        return (0,stock_code_list)

    except Exception:
        return (99,[])

def stock_craw_page():
    seen_codes_market = set()
    global_data = []
    lock = threading.Lock()
    failed_pages = []
    current_threads = 8
    with ThreadPoolExecutor(max_workers=current_threads) as executor:
        success_threads = 0
        failed_threads = 0
        total_page = stock_page_get()
        futures = {executor.submit(crawl_single_page,page): page for page in range(1,total_page+1)}
        for future in as_completed(futures):
            page = futures[future]
            try:
                status,df = future.result()
                if status == 0 and df:
                    success_threads += 1
                    failed_threads = 0
                    if success_threads >= 5 and current_threads < 10:
                        current_threads += 1
                        success_threads = 0
                    with lock:
                        new_data = [item for item in df if
                                    (item['code'], item['market']) not in seen_codes_market]
                    if new_data:
                        global_data.extend(new_data)
                        seen_codes_market.update((item['code'], item['market']) for item in new_data)
                        print(page)

                else:
                    failed_pages.append(page)
                    failed_threads += 1
                    success_threads = 0
                    print(f"{page}:{status}:try again")
                    if failed_threads >=2 and current_threads > 6:
                        current_threads -= 1
                        failed_threads = 0
            except Exception:
                failed_pages.append(page)
                print(f"page:{page}:error")


        print(f"failed_len:{len(failed_pages)}")
    if failed_pages:
        with ThreadPoolExecutor(max_workers=current_threads) as executor:
            futures = {executor.submit(crawl_single_page,page): page for page in failed_pages}
            for future in as_completed(futures):
                page = futures[future]
                try:
                    status,df = future.result()
                    if status == 0 and not df.empty:
                        with lock:
                            new_data = [item for item in df if
                                        (item['code'], item['market']) not in seen_codes_market]
                        if new_data:
                            global_data.extend(new_data)
                            seen_codes_market.update((item['code'], item['market']) for item in new_data)
                        print(page)
                except Exception:
                    failed_pages.append(page)
                    print(f"page:{page}:error")

    if global_data:
        final_df = pd.DataFrame(global_data)
        final_df.drop_duplicates(subset=['code','market'],keep='last',inplace=True)
        final_df_len=len(final_df)+1
        final_df.to_csv('stock_craw.csv',index=False,mode='w')
        print("get!,len:",final_df_len)
    else:
        print("no stock craw")

def crawl_single_page(page):
    retry_count = 0
    while retry_count < 5:  # 单页重试次数减少（并发下无需过多重试）
        status, df = stock_craw_code(page)
        if status == 0:
            return (0, df)
        elif status == 1:
            retry_count += 1
            sleep(uniform(0.2, 0.6))# 重试间隔缩短
        elif status in (2,3):
            retry_count += 3
            sleep(uniform(0.1, 0.4))
        else:
            return (status, [])
    return (99, [])

if __name__ == '__main__':
    import time
    print(stock_page_get())
    start_time = time.time()
    stock_craw_page()
    end_time = time.time()
    print("time:",end_time-start_time)