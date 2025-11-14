import pandas as pd

# d) FI and FX instrument trades & All PHASE2 products (Sheet Name -> Phase1 & Phase2 products(phase_ref))
# matching the excel trade columns to phase 1 and 2 reference sheet
PHASE1_PRODUCTS = [
    ("CURR", "FXD", "FXD"),      # Spot-Forward
    ("CURR", "FXD", "SWLEG"),    # Forex-Swap Leg
    ("IRD", "BOND", ""),         # Bonds
    ("IRD", "BOND", "CALL"),     # Callable Bonds
]

PHASE2_PRODUCTS = [
    # Commodity
    ("COM", "ASIAN", ""),
    ("COM", "FUT", ""),
    ("COM", "OFUT", "LST"),
    ("COM", "OPT", "SMP"),
    ("COM", "SWAP", ""),
    ("COM", "SWAP", "CLR"),
    # FX Option
    ("CURR", "OPT", "BAR"),
    ("CURR", "OPT", "FLEX"),
    ("CURR", "OPT", "RBT"),
    ("CURR", "OPT", "SMP"),
    ("CURR", "OPT", "SMPS"),
    # Equity and Futures
    ("EQD", "EQS", ""),
    ("EQD", "EQUIT", ""),
    ("EQD", "EQUIT", "FWD"),
    ("EQD", "FUT", ""),
    ("EQD", "OPT", "ACC"),
    ("EQD", "OPT", "ASI"),
    ("EQD", "OPT", "BAR"),
    ("EQD", "OPT", "CRAC"),
    ("EQD", "OPT", "FLEX"),
    ("EQD", "OPT", "ORG"),
    ("EQD", "OPT", "OTC"),
    ("EQD", "WARNT", ""),
    # Interest Derivatives
    ("IRD", "CF", ""),
    ("IRD", "CS", ""),
    ("IRD", "IRS", ""),
    ("IRD", "LFUT", ""),
    ("IRD", "OSWP", ""),
    ("IRD", "REPO", ""),
    ("IRD", "REPO", "REPO"),
    ("IRD", "SFUT", ""),
    # Money Markets
    ("IRD", "LN_BR", ""),
    # Credit
    ("CRD", "CDS", ""),
    ("CRD", "RTRS", ""),
]

EXCLUDE_PRODUCTS = [("EQD", "OPT", "AUTOC")]

def filter_phase_products(trades: pd.DataFrame) -> pd.DataFrame:
    trades.columns = trades.columns.str.strip().str.lower()

    trades["key"] = (
        trades["trade_fmly"].fillna('') + "|" +
        trades["trade_grp"].fillna('') + "|" +
        trades["trade_type"].fillna('')
    ).str.upper()

    valid_keys = [f"{a}|{b}|{c}" for (a, b, c) in PHASE1_PRODUCTS + PHASE2_PRODUCTS]
    exclude_keys = [f"{a}|{b}|{c}" for (a, b, c) in EXCLUDE_PRODUCTS]

    mask = trades["key"].isin(valid_keys) & (~trades["key"].isin(exclude_keys))
    return trades[mask].copy()



def detect_floor_ceiling(df):

    df_1 = df[
    (df["internal"].str.upper() == "N") &
    (df["trade_type"].str.upper() != "AUTOC")
].copy()
# single day buy trade, FI, FX, all product?
    df_1["Date_time"] = pd.to_datetime(
        df["deal_date"].astype(str) + " " + df["trade_insertion_time"].astype(str),
        errors="coerce"
    )

    df_1=df_1.sort_values("Date_time").reset_index(drop=True)
    daily_close = df_1.groupby('deal_date')['notional'].last().rename('Close_Price').reset_index()
    daily_close['Prev_Close'] = daily_close.groupby('deal_date')['Close_Price'].shift(1)
    df_1 = df_1.merge(daily_close, on='deal_date', how='left')

    #floor ceiling 
    filtered_FX = df_1[(df_1["trade_type"].str.upper() == "FXD" | df_1["trade_type"].str.upper()
                      == "SWLEG") & df_1["notional"] >= 15000000]
    filtered_FI = df_1[df_1["trade_type"].str.upper() == "CALL" | (df_1["trade_type"] == " " & 
        df_1["trade_grp"].str.upper() == "BOND") & df_1["notional"] >= 15000000]


    x_floor=0.5

    #what is the diff between buy trade and sell price
    df_1['buy_trade']=((df_1["notional"]-df_1["Prev_close"])/df_1["Prev_close"])*100
    df_1["floor_x"]=df_1.apply(lambda row: 1 if abs(df_1["buy_trade"])<= x_floor else 0, axis=1)
#still need to include the part on trade<=3 and step 6

    count_df = df_1.groupby("instrument_id")["trade_id"].count().reset_index(name="trade_count")
    df_1 = df_1.merge(count_df, on="instrument_id", how="left")
    df_1 = df_1[df_1["trade_count"] >= 3]

    if "leg_type" in df_1.columns:
        df_1 = df_1[df_1["leg_type"].str.upper() != "FARLEG"]

    df_1["fraud_type"] = "Floor_Ceiling"
    return df_1



def detect_ramping(df):
    x_ramp=35
    daily_first = df_1.groupby('Date')['notional'].first().rename('Open_Price').reset_index()
    df_1 = df_1.merge(daily_first, on='Date', how='left')
    df_1['Open_Price'] = df_1['Open_Price'].fillna(df_1['Prev_Close'])
    rate_ramp= (df_1['rate']-df_1['Open_Price'])*100/df_1['Open_Price']
    


def detect_all_fraud(trades_df):

    df = filter_phase_products(trades_df)
    trades_df["fraud_type"] = "None"

    floor_ceiling = detect_floor_ceiling(df)
    # ramping = detect_ramping(df)

    frauds = pd.concat([floor_ceiling, ramping], ignore_index=True).drop_duplicates("trade_id")

    merged = trades_df.merge(frauds, on="trade_id", how="left", suffixes=("", "_detected"))
    merged["fraud_type"] = merged["fraud_type_detected"].fillna(merged["fraud_type"])
    merged.drop(columns=["fraud_type_detected"], inplace=True)
    # merge data with new fraud_type column

    return merged


if __name__ == "__main__":
    trade_file = "sample data.xlsx"
    trades = pd.read_excel(trade_file)

    classified = detect_all_fraud(trades)
    classified.to_excel("classified_trades.xlsx", index=False)

    print(f" Classified {len(classified)} trades into fraud types.")

