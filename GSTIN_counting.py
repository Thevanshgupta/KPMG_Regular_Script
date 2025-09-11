import os
import csv
from datetime import datetime

root_folder = r"E:\GSTR2B Daily Data"
output_csv = r"E:\GSTR2B_Report.csv"  # Write CSV outside to avoid file lock

month_map = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"
}

rows = []

print(f"Looking in: {root_folder}")
for date_folder in os.listdir(root_folder):
    date_folder_path = os.path.join(root_folder, date_folder)

    if not os.path.isdir(date_folder_path):
        continue

    try:
        date_obj = datetime.strptime(date_folder, "%d-%m-%Y")  # <-- fixed format
        date_str = date_obj.strftime("%d-%m-%Y")
    except ValueError:
        print(f"Skipping folder due to invalid date format: {date_folder}")
        continue

    print(f"\nProcessing folder: {date_folder}")

    for subfolder in os.listdir(date_folder_path):
        subfolder_path = os.path.join(date_folder_path, subfolder)

        if not os.path.isdir(subfolder_path):
            continue

        for filename in os.listdir(subfolder_path):
            if filename.endswith(".json"):
                try:
                    print(f"  Found JSON: {filename}")
                    name_parts = filename.replace(".json", "").split("_")
                    gstin = name_parts[0]
                    period = name_parts[1]
                    month = period[:2]
                    year = period[2:]

                    tax_period = f"{month_map.get(month, '??')}-{year[-2:]}"
                    fin_year = f"{str(int(year[-2:])-1)}-{year[-2:]}"

                    rows.append([
                        gstin,
                        tax_period,
                        date_str,
                        date_folder,
                        fin_year
                    ])
                except Exception as e:
                    print(f"  Error parsing filename {filename}: {e}")

# Write to CSV
with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["GSTIN", "Tax Period", "Date", "Folder", "Financial Year"])
    writer.writerows(rows)

print(f"\n✅ CSV written to: {output_csv}")
