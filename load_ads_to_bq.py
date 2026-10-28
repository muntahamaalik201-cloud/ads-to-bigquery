import os
import re
import hashlib
import base64
import math
from datetime import datetime, timezone

import google.auth
import pandas as pd
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from supabase import create_client, Client

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/cloud-platform",
]

TABLE_BY_AD_TYPE = {
    "image": "image_ads",
    "video": "video_ads",
    "text": "text_ads",
}

SELECTED_COLUMNS_BY_AD_TYPE = {
    "image": ["advertiser", "name", "app_link", "image_url", "claim_time"],
    "video": ["advertiser", "name", "app_link", "youtube_url", "claim_time", "views"],
    "text": ["advertiser", "name", "app_link", "headline", "claim_time", "description"],
}

METADATA_COLUMNS = [
    "_row_hash", "_source_spreadsheet_id", "_source_tab", 
    "_source_row_number", "_ad_type_raw", "_ad_type", "_loaded_at_utc"
]

def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()

def hex_to_youtube_url(video_url: str) -> tuple[str, str]:
    video_url = str(video_url or "").strip()
    match = re.search(r"([a-fA-F0-9]{16})", video_url)
    if not match:
        return "", ""
    
    hex_id = match.group(1).lower()
    try:
        b64_id = base64.urlsafe_b64encode(bytes.fromhex(hex_id)).decode('utf-8').rstrip('=')
        return f"https://www.youtube.com/watch?v={b64_id}", hex_id
    except ValueError:
        return "", hex_id

def make_row_hash(row, hash_columns: list) -> str:
    raw = "|".join(str(row.get(col, "") or "").strip().lower() for col in hash_columns)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def get_hash_columns(ad_type: str) -> list[str]:
    if ad_type == "image":
        return ["image_url"]
    if ad_type == "video":
        return ["advertiser", "_ad_type_raw"]
    # NEW: Text ads now hash based on their actual content
    if ad_type == "text":
        return ["advertiser", "headline", "description"]
    
    return ["_source_spreadsheet_id", "_source_tab", "_source_row_number", "_ad_type_raw", "_ad_type"]

def make_current_duplicate_key(row, ad_type: str):
    def clean(val): return str(val or "").strip().lower()
    
    if ad_type == "image":
        return (clean(row.get("image_url", "")),)
    if ad_type == "video":
        return (clean(row.get("advertiser", "")), clean(row.get("_ad_type_raw", "")))
    # NEW: Text ads duplicate key uses content instead of _row_hash
    if ad_type == "text":
        return (clean(row.get("advertiser", "")), clean(row.get("headline", "")), clean(row.get("description", "")))
        
    return (clean(row.get("_row_hash", "")),)

def get_existing_duplicate_keys(bq_client, table_id, ad_type):
    try:
        table = bq_client.get_table(table_id)
    except NotFound:
        return set()

    existing_schema_by_name = {field.name: field for field in table.schema}
    key_columns = get_hash_columns(ad_type)
    
    # REMOVE OR COMMENT OUT the old text override:
    # if ad_type == "text":
    #     key_columns = ["_row_hash"]

    selected_fields = [existing_schema_by_name[col] for col in key_columns if col in existing_schema_by_name]
    if len(selected_fields) != len(key_columns):
        return set()

    rows = bq_client.list_rows(table, selected_fields=selected_fields, page_size=10000)
    existing_keys = set()
    for row in rows:
        values = tuple(str(row[col] or "").strip().lower() for col in key_columns)
        if any(values):
            existing_keys.add(values)
    return existing_keys
def ensure_table_exists_and_schema(bq_client, table_id, schema):
    try:
        table = bq_client.get_table(table_id)
        existing_field_names = {field.name for field in table.schema}
        missing_fields = [f for f in schema if f.name not in existing_field_names]
        if missing_fields:
            table.schema = list(table.schema) + missing_fields
            bq_client.update_table(table, ["schema"])
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)

def get_sheet_id_by_name(sheets_service, spreadsheet_id, tab_name):
    sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in sheet_metadata.get('sheets', []):
        if sheet['properties']['title'] == tab_name:
            return sheet['properties']['sheetId']
    raise ValueError(f"Tab '{tab_name}' not found.")

def filter_dataframe_for_new_rows(bq_client, df, project_id, dataset_id, table_name, ad_type) -> pd.DataFrame:
    if df.empty:
        return df

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    df["_dedupe_key"] = df.apply(lambda row: make_current_duplicate_key(row, ad_type), axis=1)
    df = df[~df["_dedupe_key"].apply(lambda k: not any(str(v).strip() for v in k))].copy()
    df = df.drop_duplicates(subset=["_dedupe_key"]).copy()

    existing_keys = get_existing_duplicate_keys(bq_client, table_id, ad_type)
    if existing_keys:
        df = df[~df["_dedupe_key"].isin(existing_keys)].copy()

    df = df.drop(columns=["_dedupe_key"], errors="ignore")
    return df

def load_dataframe_to_bigquery(bq_client, df, project_id, dataset_id, table_name, schema):
    if df.empty:
        return
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    ensure_table_exists_and_schema(bq_client, table_id, schema)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    bq_client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"Appended {len(df)} new rows into BigQuery table {table_id}")

def load_dataframe_to_supabase(supabase_client: Client, df: pd.DataFrame, table_name: str):
    if df.empty:
        return
        
    df_clean = df.copy()
    
    # Format datetime for JSON/PostgreSQL
    if '_loaded_at_utc' in df_clean.columns:
        df_clean['_loaded_at_utc'] = df_clean['_loaded_at_utc'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
    # Handle NaNs and convert nullable types to Python natives
    for col in df_clean.columns:
        if df_clean[col].dtype == 'Int64':
            df_clean[col] = df_clean[col].apply(lambda x: int(x) if pd.notnull(x) else None)
        else:
            df_clean[col] = df_clean[col].replace({float('nan'): None, pd.NA: None})

    records = df_clean.to_dict(orient="records")
    
    try:
        supabase_client.table(table_name).insert(records).execute()
        print(f"Appended {len(records)} new rows into Supabase table {table_name}")
    except Exception as e:
        print(f"Error inserting into Supabase table {table_name}: {e}")
def build_app_link(package_name):
    pkg_str = str(package_name).strip()
    if not pkg_str:
        return ""
    if pkg_str.startswith("id"):
        return f"https://apps.apple.com/us/app/{pkg_str}"
    return f"https://play.google.com/store/apps/details?id={pkg_str}"
def main():
    project_id = required_env("GCP_PROJECT_ID")
    dataset_id = required_env("BQ_DATASET")
    spreadsheet_id = required_env("SPREADSHEET_ID")
    tab_name = required_env("TAB_NAME")
    
    supabase_url = required_env("SUPABASE_URL")
    supabase_key = required_env("SUPABASE_KEY")
    supabase_client = create_client(supabase_url, supabase_key)

    credentials, _ = google.auth.default(scopes=SCOPES)
    sheets_service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    bq_client = bigquery.Client(project=project_id, credentials=credentials)

    response = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A:ZZ"
    ).execute()
    
    values = response.get("values", [])
    if not values or len(values) < 2:
        print("No data found to process.")
        return

    headers = [str(h).strip().lower() for h in values[0]]
    df = pd.DataFrame(values[1:], columns=headers[:len(values[1])])
    
    required_cols = ["package_name", "video_url", "image_url", "headline", "description", "advertiser_name", "ad_type"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    df["_source_row_number"] = range(2, len(df) + 2)
    
    df["advertiser"] = df["advertiser_name"]
    df["name"] = df["package_name"]
    df["app_link"] = df["package_name"].apply(build_app_link)
    
    yt_data = df["video_url"].apply(hex_to_youtube_url)
    df["youtube_url"] = [x[0] for x in yt_data]
    df["_ad_type_raw"] = [x[1] for x in yt_data]
    df["_ad_type"] = df["ad_type"].str.strip().str.lower()
    df["claim_time"] = ""
    df["views"] = ""
    
    df["_source_spreadsheet_id"] = spreadsheet_id
    df["_source_tab"] = tab_name
    df["_loaded_at_utc"] = datetime.now(timezone.utc)

    for ad_type, table_name in TABLE_BY_AD_TYPE.items():
        filtered_df = df[df["_ad_type"] == ad_type].copy()
        if filtered_df.empty:
            continue
            
        selected_cols = SELECTED_COLUMNS_BY_AD_TYPE[ad_type]
        hash_columns = get_hash_columns(ad_type)
        filtered_df["_row_hash"] = filtered_df.apply(lambda row: make_row_hash(row, hash_columns), axis=1)
        
        final_cols = selected_cols + METADATA_COLUMNS
        final_df = filtered_df[final_cols].copy()
        
        for col in selected_cols + [c for c in METADATA_COLUMNS if c not in ["_source_row_number", "_loaded_at_utc"]]:
            final_df[col] = final_df[col].astype(str).replace("nan", "")
            
        final_df["_source_row_number"] = pd.to_numeric(final_df["_source_row_number"], errors="coerce").astype("Int64")
        final_df["_loaded_at_utc"] = pd.to_datetime(final_df["_loaded_at_utc"], errors="coerce", utc=True)

        # Extract only the genuinely new rows (using BigQuery as the source of truth for duplicates)
        new_rows_df = filter_dataframe_for_new_rows(
            bq_client=bq_client, 
            df=final_df, 
            project_id=project_id, 
            dataset_id=dataset_id, 
            table_name=table_name, 
            ad_type=ad_type
        )

        if new_rows_df.empty:
            print(f"No new rows to insert for {table_name}.")
            continue

        schema = []
        for col in new_rows_df.columns:
            if col == "_loaded_at_utc":
                schema.append(bigquery.SchemaField(col, "TIMESTAMP"))
            elif col == "_source_row_number":
                schema.append(bigquery.SchemaField(col, "INTEGER"))
            else:
                schema.append(bigquery.SchemaField(col, "STRING"))

        # Load identical datasets to both destinations
        load_dataframe_to_bigquery(bq_client, new_rows_df, project_id, dataset_id, table_name, schema)
        load_dataframe_to_supabase(supabase_client, new_rows_df, table_name)

    print("Pipeline completion successful. Deleting processed rows from the Google Sheet...")
    sheet_id = get_sheet_id_by_name(sheets_service, spreadsheet_id, tab_name)
    
    delete_request = {
        "deleteDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": 1, 
                "endIndex": len(values)
            }
        }
    }
    
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [delete_request]}
    ).execute()
    
    print(f"Successfully deleted rows 2 through {len(values)} from '{tab_name}'.")

if __name__ == "__main__":
    main()
