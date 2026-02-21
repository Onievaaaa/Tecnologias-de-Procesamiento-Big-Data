import pandas as pd
import matplotlib.pyplot as plt
import os


csv_path = "data.csv"
out_dir = "plots"

os.makedirs(out_dir, exist_ok=True)


df = pd.read_csv(csv_path)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

metricas = ["sma_200", "ema_50", "rsi_14", "macd_12_26"]


for m in metricas:
    plt.figure(figsize=(12,4))
    plt.plot(df["date"], df[m])
    plt.title(m)
    plt.grid(True)
    plt.savefig(f"{out_dir}/{m}.png")
    plt.close()

