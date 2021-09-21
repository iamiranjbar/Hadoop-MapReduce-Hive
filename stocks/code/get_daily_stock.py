import os
import requests
import datetime
import shutil
import pandas as pd
from persiantools.jdatetime import JalaliDate

TEMP_DIRECTORY = "temp"
RESULT_DIRECTORY = "results"
base_url = "http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d={}-{}-{}"

if os.path.exists(TEMP_DIRECTORY):
    shutil.rmtree(TEMP_DIRECTORY)
os.mkdir(TEMP_DIRECTORY)

if os.path.exists(RESULT_DIRECTORY):
    shutil.rmtree(RESULT_DIRECTORY)
os.mkdir(RESULT_DIRECTORY)


today_date = JalaliDate.today()
six_month_ago = JalaliDate(1399, 7, 26)
current_date = six_month_ago

while current_date != today_date:
    url = base_url.format(current_date.year, current_date.month, current_date.day)
    file_name = "stock_data_{}_{}_{}.xlsx".format(current_date.year, current_date.month, current_date.day)
    file_path = os.path.join(TEMP_DIRECTORY, file_name)
    response = requests.get(url)
    open(file_path, 'wb').write(response.content)
    current_date += datetime.timedelta(days=1)
    print("{} has been downloaded.".format(file_name))
    # Remove files under 10 kb (Stock close days)
    if os.path.getsize(file_path) < 10 * 1024:
        os.remove(file_path)
        continue
    # Read it with pandas, omit two first rows, add date column and convert to csv
    current_date_data = pd.read_excel(file_path)
    header = current_date_data.iloc[1]
    current_date_data = current_date_data.iloc[2:]
    current_date_data.columns = header
    current_date_data['تاریخ'] = "{}/{}/{}".format(current_date.year, current_date.month, current_date.day)
    result_file_name = "stock_data_{}_{}_{}.csv".format(current_date.year, current_date.month, current_date.day)
    result_path = os.path.join(RESULT_DIRECTORY, result_file_name)
    current_date_data.to_csv(result_path, encoding='utf-8', index=False)
