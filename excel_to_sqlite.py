import pandas as pd
import sqlite3



DATE_COL_MAP = {
    "Strategic Data": ["Time Period"],
    "Order Date": ["ORDER_DATE"],
    "Customer Shipment": ["SHIPMENT_TIME_PERIOD_ID"],
    "Material Forecast": ["TIME_PERIOD_ID", "FORECAST_PERIOD"],
    "Facility Material Inventory": ["TIME_PERIOD_ID"],
    "Lost Sale": ["Lost_Month"],
    "Promotion": ["PROMOTION_START_DT", "PROMOTION_END_DT"]
}

# Purpose:
# Ye ek mapping hai: Kaunse Excel sheet/table me kaunse columns "date"-like hain,jinhe string/text format ("Jan-22" etc.) me bhi chahiye hoga.

def excel_to_sqlite(excel_path: str, sqlite_path: str = "supply_chain_data.db"):
    xls = pd.ExcelFile(excel_path) #pd.ExcelFile() se pura Excel workbook load hota hai
    conn = sqlite3.connect(sqlite_path) #sqlite3.connect() DB file bana deta hai (agar pehle nahi hai toh)
# Tum jab bhi sqlite3.connect("supply_chain_data.db") run karte ho:
# Agar file hai: toh usko open kar lega
# Agar file nahi hai: toh automatically create kar dega (same folder me)
    for sheet in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name=sheet)
        date_cols = DATE_COL_MAP.get(sheet, [])
        for col in date_cols:
            if col in df.columns:
                try:
                    dt_col = pd.to_datetime(df[col], errors='coerce')
                    # toh jo bhi value convert nahi ho sakti usko pandas “NaT” bana deta hai
                    # (NaT = Not a Time, yaani missing/invalid datetime).
                    df[f"{col}_Text"] = dt_col.dt.strftime("%b-%y") #Fir ek naya column bana raha hai: "TimePeriod_Text" jisme format hoga "Jan-22"
                    # .dt.strftime() har date ko ek string bana deta hai,
                    # format: "%b-%y" matlab:
                    # %b = Month abbreviation ("Jan", "Feb", "Mar" ...)
                    # %y = 2-digit year ("22", "23", ...)
                    # So, "2022-01-01" → "Jan-22"
                    print(f"Added {col}_Text to {sheet}")
                except Exception as e:
                    print(f"Warning: Could not convert {col} in {sheet}: {e}")
        df.to_sql(sheet, conn, if_exists="replace", index=False)
        #Jo DataFrame bana usko direct SQLite DB me as table store kar diya
        #if_exists="replace" means — agar pehle se table hai toh overwrite kar dega
    conn.close()

if __name__ == "__main__":
    excel_to_sqlite("supply_chain_data.xlsx")






