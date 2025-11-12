import asyncio
from random import uniform,choice
from httpx import AsyncClient
import tqdm
import json
import pandas as pd

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0"
        # 可继续添加更多User-Agent
]

async def init_client():
    return AsyncClient(
        http2=True,
        verify=False,
        headers = {
            'User-Agent': choice(USER_AGENTS),
            'Referer': 'https://quote.eastmoney.com/',
            'Accept-Encoding': choice(['gzip','deflate','br','zstd']),
            # 'Accept-Language': 'zh-TW,zh-CN;q=0.9,zh;q=0.8,en;q=0.7,en-GB;q=0.6,en-US;q=0.5',
            'Connection': 'keep-alive',
        }
    )

async def get_weekly_kline(client,stock_code, market_code, weeks=100):
    """
    获取股票的周线K线数据
    :param stock_code: 股票代码（如600000）
    :param market_code: 市场代码（沪市=1，深市=0）
    :param weeks: 获取的周数（默认100周，需大于BOLL周期20周）
    :return: 包含日期和周收盘价的DataFrame
    """
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get/"

    params = {
        "secid": f"{market_code}.{stock_code}",  # 股票唯一标识
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",  # 固定令牌
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f53",  # f51=周日期，f55=周收盘价
        "klt": "102",  # 周线（102=周线，区别于日线101）
        "fqt": "1",  # 前复权（保持与东方财富网页面一致）
        "end": "20500101",  # 获取到最新周线
        "lmt": 120 # 获取的周线数量（如100周）
    }

    headers = {
        'User-Agent': choice(USER_AGENTS),
    }

    try:
        response = await client.get(url=url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        # 处理JSONP格式
        #jsonp_data = response.text
        #print(jsonp_data)
        # pure_json = re.search(r'jQuery\w+\((.*)\)', jsonp_data).group(1)
        # if pure_json:
        #     print("pure_json", pure_json)
        # else:
        #     print("error")
        data = json.loads(response.text)

        # 提取周线数据（每条K线对应一周）
        klines = data["data"]["klines"]
        weekly_list = []
        for line in klines:
            parts = line.split(',')
            weekly_list.append({
                "week_date": parts[0],  # 周日期（通常是周五）
                "weekly_close": float(parts[1])  # 周收盘价（f55字段）
            })

        # 转为DataFrame并按周排序（从旧到新）
        df = pd.DataFrame(weekly_list)
        df["week_date"] = pd.to_datetime(df["week_date"])
        df = df.sort_values("week_date").reset_index(drop=True)
        return df.tail(weeks)

    except Exception as e:
        return None


def calculate_weekly_boll(df, n=20, k=2):
    """
    计算周线BOLL指标（基于20周数据）
    :param df: 包含周收盘价的DataFrame（需有"weekly_close"列）
    :param n: 周期（默认20周）
    :param k: 标准差倍数（默认2倍）
    :return: 新增周线中轨、上轨、下轨的DataFrame
    """
    if df is None or len(df) < n:
        return None

    # 计算中轨（20周移动平均）
    df["boll_mid_weekly"] = df["weekly_close"].rolling(window=n).mean()

    # 计算20周内的标准差
    std_weekly = df["weekly_close"].rolling(window=n).std()

    # 计算上轨和下轨
    df["boll_up_weekly"] = df["boll_mid_weekly"] + k * std_weekly
    df["boll_dn_weekly"] = df["boll_mid_weekly"] - k * std_weekly

    return df.tail(1).copy()

async def stock_csv_get():
    # 示例：获取贵州茅台（600519，沪市market_code=1）的周线BOLL
    # 获取最近120周的周线数据（需大于20周，确保BOLL计算有效）
    df = pd.read_csv(r"stock_craw.csv",dtype={'code':str})
    dfs = [(row['market'],row['code']) for _, row in df.iterrows()]
    client = await init_client()

    semaphore = asyncio.Semaphore(10)

    async def process_one(market_code, stock_code):
        async with semaphore:  # 限制并发
            # 重试逻辑同上
            for retry in range(3):
                weekly_df = await get_weekly_kline(client, stock_code, market_code)
                if weekly_df is not None:
                    # 计算BOLL并返回结果
                    boll_df = calculate_weekly_boll(weekly_df)
                    if boll_df is not None:
                        row = boll_df.to_dict('records')[0]
                        row['code'] = stock_code
                        return row
                    else:
                        return {
                            'week_date': weekly_df['week_date'].iloc[-1],
                            'weekly_close': weekly_df['weekly_close'].iloc[-1],
                            "boll_mid_weekly": None,
                            "boll_up_weekly": None,
                            "boll_dn_weekly": None,
                            'code': stock_code,
                        }
                await asyncio.sleep(uniform(0.3, 0.7))  # 异步延迟
            return  {
            "week_date": None,
            "weekly_close": None,
            "boll_mid_weekly": None,
            "boll_up_weekly": None,
            "boll_dn_weekly": None,
            "code": stock_code,
            }

    tasks = [process_one(market_code, stock_code) for market_code, stock_code in dfs]
    result = []
    for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks),desc="async"):
        res = await f
        if res is not None:
            result.append(res)



    # for df in dfs:
    #     df.dropna(inplace=True)

    big_df = pd.DataFrame(result).dropna(subset=['code'])
    big_df.to_csv("stock_code.csv", index=False, mode='w')

if __name__ == '__main__':
    async def main():
        client = await init_client()
        result = await get_weekly_kline(client,'002333','0')
        print(result)
    asyncio.run(main())