import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
def load_to_supabase():
    csv_path = "../data/staged/NasaData_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")
    df = pd.read_csv(csv_path)
    df["inserted_at"] = pd.to_datetime(df["inserted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    batch_size = 20
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].where(pd.notnull(df), None).to_dict("records")
        values = [
            f"""('{r['date']}', 
            '{str(r['title']).replace("'", "''")}', 
            '{str(r['description']).replace("'", "''")}', 
            '{r.get('media_type', 'NULL')}', 
            '{r.get('image_url', 'NULL')}', 
            '{r['inserted_at']}')"""
            for r in batch
        ]
        insert_sql = f"""
        INSERT INTO nasa_apod(date, title, explanation, media_type, image_url, inserted_at)
        VALUES {",".join(values)}
        """
        try:
            response = supabase.rpc("execute_sql", {"query": insert_sql}).execute()
            error = getattr(response, "error", None) or getattr(response, "msg", None)
            if error:
                print(f"Error inserting rows {i+1} --- {min(i + batch_size, len(df))}: {error}")
            else:
                print(f"Inserted rows {i+1} --- {min(i + batch_size, len(df))}")
        except Exception as e:
            print(f"Exception occurred while inserting: {e}")
    print("Finished Loading NASA data.")
if __name__ == "__main__":
    load_to_supabase()
