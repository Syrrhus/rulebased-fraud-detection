import pandas as pd

def classify(df):

    df_1=df[df["internal"].upper()=="N" & df_1["trade_type"]!= "AUTOC"]# single day buy trade, FI, FX, all product?
    df_1["Date_time"]= pd.to_datetime(df['deal_date'] + ' ' + df['trade_insertion_time'])

    df_1=df_1.sort_values("Date_time").reset_index(drop=True)
    daily_close = df_1.groupby('Date')['notional'].last().rename('Close_Price').reset_index()
    daily_close['Prev_Close'] = daily_close.groupby('deal_date')['Close_Price'].shift(1)
    df_1 = df_1.merge(daily_close, on='Date', how='left')

    

    #floor ceiling 
    filtered_FX = df_1[(df_1["trade_type"].upper() == "FXD" | df_1["trade_type"].upper()
                      == "SWLEG") & df_1["notional"] >= 15000000]
    filtered_FI = df_1[df_1["trade_type"].upper() == "CALL" | (df_1["trade_type"] == " " & 
        df_1["trade_grp"].upper() == "BOND") & df_1["notional"] >= 15000000]


    x_floor=0.5

    #what is the diff between buy trade and sell price
    df_1['buy_trade']=((df_1["notional"]-df_1["Prev_close"])/df_1["Prev_close"])*100
    df_1["floor_x"]=df_1.apply(lambda row: 1 if abs(df_1["buy_trade"])<= x_floor else 0, axis=1)
#still need to include the part on trade<=3 and step 6


    #Ramp
    x_ramp=35
    daily_first = df_1.groupby('Date')['notional'].first().rename('Open_Price').reset_index()
    df_1 = df_1.merge(daily_first, on='Date', how='left')
    df_1['Open_Price'] = df_1['Open_Price'].fillna(df_1['Prev_Close'])
    rate_ramp= (df_1['rate']-df_1['Open_Price'])*100/df_1['Open_Price']
    


    

