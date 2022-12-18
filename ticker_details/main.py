import polygon
import pandas as pd
import concurrent.futures
from sqlalchemy import create_engine
import time
import config
from datetime import datetime


def get_data(symbol):
    df1 = pd.DataFrame()
    try:
        resp = reference_client.get_ticker_details(symbol, date=datetime.today().date())['results']
        df1 = pd.DataFrame([resp])
        print("getting data for: ", symbol)
    except Exception:
        print(Exception)
        pass
    return df1


if __name__ == "__main__":
    df2 = pd.DataFrame()
    now = time.time()
    reference_client = polygon.ReferenceClient(config.polygon_key)  # for usual sync client
    engine = create_engine(config.psql)

    # Get Symbols
    df = pd.read_csv("https://files.catnmsplan.com/symbol-master/FINRACATReportableEquitySecurities_EOD.txt", sep="|",
                     header=None, names=["Symbol", "Issue_Name", "Primary_Listing_Mkt", "test_issue_flag"])
    df.drop(0, axis=0, inplace=True)
    df = df[df.test_issue_flag == 'N'].reset_index(drop=True)  # Drop test Data
    df = df[df.Primary_Listing_Mkt != 'U'].reset_index(drop=True)  # Drop Pink Sheets
    symbols = df['Symbol'].to_list()
    symbols = [d for d in symbols if type(d) == str]
    symbols = [x.replace(' ', '.') for x in symbols]

    # Get Details
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(get_data, symbols)
        for result in results:
            try:
                df2 = pd.concat([df2, result])
            except Exception:
                pass

    # Format Details
    df2['branding'] = df2['branding'].astype('string')  # or keep as JSON
    df2['address'] = df2['address'].astype('string')    # or keep as JSON

    # Save to DB
    print("Adding to database...")
    df2.to_sql('companies', engine, if_exists='replace', index=False)
    print("Done")
    print(f"time: {now - time.time()}")


