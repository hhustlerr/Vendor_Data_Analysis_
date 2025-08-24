import psycopg2
import pandas as pd
import logging

logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

def create_vendor_summary(conn):
    ''' This function will merge differnt tablesto get the overall vendor summary and adding new columns in the vendor data'''
    vendor_sales_summary = pd.read_sql("""select
    pp."VendorName",
    pp."VendorNumber",
    pp."Brand",
    pp."Volume",
    pp."Price" as "ActualPrice",
    pp."PurchasePrice",
    sum(vi."Quantity") as "TotalPurchaseQuantity",
    sum(vi."Dollars") as "TotalPuhchaseDollars",
    sum(s."SalesDollars") as "TotalSalesDollar",
    sum(s."SalesPrice") as "TotalSalesPrice", 
    sum(s."SalesQuantity") as "TotalSalesQuantity",
    sum(s."ExciseTax") as "TotalExciseTax",
    sum(vi."Freight") as "FreightCost"
    from purchase_prices pp
    join sales s
    on pp."Brand" = s."Brand"
    and pp."VendorNumber" = s."VendorNo"
    join vendor_invoice vi
    on pp."VendorNumber" = vi."VendorNumber"
    group by  pp."Volume", pp."VendorName", pp."VendorNumber", pp."Brand", pp."Price", pp."PurchasePrice" """, engine)

    return create_vendor_summary

def clean_data(df):
    '''this function will clean the data'''
    
    # chnging the dtype from obj to float
    df['Volume'] = df["Volume"].astype('float')  

   # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()

   # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollar'] - df['TotalPuhchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] - df['TotalSalesDollar'])*100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollar'] / df['TotalPuhchaseDollars']

    return df

if name == "__main__":
    # creating database connection
    conn = psycopg2.connect(
        host = "localhost",
        port = "5423",
        database = "inventory",
        user = "postgres",
        password = "password"
    )
    
    logging.info("Creating Vendor Summary Table...")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())
    
    logging.info("Cleaning data begins...")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())
    
    logging.info("Ingesting data...")
    ingest_db(clean_df, 'final_table', conn)
    logging.info('Completed')
    
    
    