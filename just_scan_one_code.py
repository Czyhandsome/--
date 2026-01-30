import akshare as ak

if __name__ == '__main__':
    start_date = "20180101"
    df = ak.stock_zh_a_hist(
        symbol='000157',
        period="daily",
        start_date=start_date,
        adjust=""
    )
    print(df)
