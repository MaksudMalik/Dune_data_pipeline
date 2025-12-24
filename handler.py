import asyncio
import httpx
import pandas as pd
import json
import os
import time

# API_KEY = os.getenv("API_KEY")
CRYPTORANK_API_KEY = os.getenv("CRYPTORANK_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
MAX_REQ_PER_MIN = 100
CONCURRENCY = 5 
DASH = "-"

async def fetch_project(client, p_id, semaphore):
    if not p_id: return None
    
    async with semaphore:
        url = f"https://api.cryptorank.io/v2/currencies/{p_id}"
        try:
            resp = await client.get(url, headers={"X-Api-Key": CRYPTORANK_API_KEY}, timeout=15.0)
            if resp.status_code != 200: 
                print(f"Failed to fetch {p_id}: {resp.status_code}")
                return None
            
            data = resp.json().get('data', {})
            ticker = data.get("symbol")
            price = data.get("price")
            
            if price is None: return {"ticker": ticker}
                
            ath = data.get("ath", {}).get("value")
            totalSupply = data.get("totalSupply")
            
            return {
                "ticker": ticker,
                "price": price,
                "totalSupply": totalSupply,
                "ath": ath,
            }
        except Exception:
            print(f"Exception occurred for {p_id}")
            return None

async def main():
    presale_data = pd.read_csv("presale_data.csv")
    semaphore = asyncio.Semaphore(CONCURRENCY)
    all_results = []

    async with httpx.AsyncClient() as client:
        for i in range(0, len(presale_data), MAX_REQ_PER_MIN):
            start_time = time.perf_counter()
            batch_ids = presale_data['id'].iloc[i : i + MAX_REQ_PER_MIN]
            tasks = [fetch_project(client, p_id, semaphore) for p_id in batch_ids]
            batch_results = await asyncio.gather(*tasks)
            print(f"Batch {i//100 + 1} done.")
            all_results.extend(batch_results)
            elapsed = time.perf_counter() - start_time
            if i + MAX_REQ_PER_MIN < len(presale_data) and elapsed < 60:
                print(f"Sleeping for {60 - elapsed:.2f}s...")
                await asyncio.sleep(60 - elapsed)

        formatted = [r if r else {"price": DASH, "totalSupply": DASH, "ath": DASH} for r in all_results]
        res_df = pd.DataFrame(formatted)
        
        for col in res_df.columns:
            presale_data[col] = res_df[col]

        presale_data.to_csv("presale_data.csv", index=False)
        print("Local CSV updated.")

            # 3. Push to Dune
        print("Pushing to Dune...")
        dune_url = "https://api.dune.com/api/v1/uploads/csv"
        csv_data = presale_data.to_csv(index=False)
        
        payload = {
            "table_name": "presale_data",
            "data": csv_data,
            "description": "Data obtained from Cryptorank API, Updates every 24 hours.",
            "is_private": False
        }
        
        dune_headers = {
            "X-DUNE-API-KEY": DUNE_API_KEY,
            "Content-Type": "application/json"
        }
        
        dune_resp = await client.post(dune_url, json=payload, headers=dune_headers, timeout=30.0)
        
        if dune_resp.status_code in [200, 201]:
            print("Success! Table uploaded to Dune.")
        else:
            print(f"Dune Failed: {dune_resp.status_code}")
            print(dune_resp.text)

if __name__ == "__main__":

    asyncio.run(main())
