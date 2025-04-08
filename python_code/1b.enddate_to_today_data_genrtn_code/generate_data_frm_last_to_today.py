from datetime import datetime as dt, date, timedelta
import random
import csv
import pandas as pd
import os

# make transaction data
def makeTransactionData(start_date, end_date, names_file_path, products_file_path, output_file_path):

    first_name = []
    last_name = []
    store = ["satara", "pune", "sangli", "nashik"]

    # Read first names and last names
    cnt = 0
    with open(names_file_path) as f:
        for line in f:
            if cnt == 0:
                cnt += 1
                continue
            fs_name, ls_name = line.strip().split(",")
            first_name.append(fs_name)
            last_name.append(ls_name)

    # Read product descriptions, categories, and prices
    description = []
    category = []
    price1 = []
    cnt1 = 0
    with open(products_file_path) as f:
        for line in f:
            if cnt1 == 0:
                cnt1 += 1
                continue
            description_name, price, category1 = line.strip().split(",")
            description.append(description_name)
            category.append(category1)
            price1.append(price)

    # Generate transactions
    current_date = start_date
    while current_date <= end_date:
        num_tran = random.randint(5, 15)
        for tran in range(num_tran):
            ls_num = random.randint(0, len(first_name) - 1)
            fs_name = first_name[ls_num]
            ls_name = last_name[ls_num]
            member_id = ls_num + 1001
            store_id = random.randint(0, len(store) - 1)
            store_name = store[store_id]
            store_id += 1
            num_of_product = random.randint(1, 10)

            for num_p in range(num_of_product):
                qty = random.randint(1, 10)
                product_num = random.randint(0, len(description) - 1)
                product_id = product_num + 1
                product = description[product_num]
                price = price1[product_num]
                cate = category[product_num]

                tranId = current_date.strftime('%Y-%m-%dT') + dt.now().strftime("%H-%M-%S-%f") + '_' + str(
                    round(tran * 965)) + '_' + str(tran)

                # Call makeDirty to write the data to CSV
                makeDirty(output_file_path, current_date, tranId, product, cate, store_name, qty, price, fs_name,
                          ls_name, member_id, product_id, store_id)

        # Move to the next day
        current_date += timedelta(days=1)

# make data dirty manually add "," in file and write csv
def makeDirty(input_filepath, start, tranId, product, cate, store_name, qty, price, fs_name, ls_name, member_id, product_id, store_id):
    with open(input_filepath, 'a', newline='') as file:
        #print(start)
        start = start.strftime('%d-%m-%Y')
        writer = csv.writer(file)
        writer.writerow([start, ",", tranId, ",", product, ",", cate, ",", store_name, ",", qty, ",", price, ",", fs_name, ",",
             ls_name, ",", member_id, ",", product_id, ",", store_id])

# clean data
def cleanData(input_filepath, output_filepath):
    lst = []

    with open(input_filepath, 'r') as f:
        # next(f)
        for line in f:
            start, _, _, tranId, _, _, product, _, _, cate, _, _, store_name, _, _, qty, _, _, price, _, _, fs_name, _, _, ls_name, _, _, member_id, _, _, product_id, _, _, store_id = line.strip().split(
                ',')
            lst.append([start, tranId, product, cate, store_name, qty, price, fs_name, ls_name, member_id, product_id, store_id])

    with open(output_filepath, 'w', newline='') as f:
        col_names = "start,tranId,description,category,store_name,qty,price,fs_name,ls_name,member_id,product_id,store_id\n"
        f.write(col_names)

        for item in lst:
            # print(item[0])
            f.write(f"{item[0]},{item[1]},{item[2]},{item[3]},{item[4]},{item[5]},{item[6]},{item[7]},{item[8]},{item[9]},{item[10]},{item[11]}\n")

# merge newly generated clean data in previous data
def mergeData(clean_data_filepath, previous_data_filepath):
    # 1: Read the previous data and clean data into DataFrames
    prev_df = pd.read_csv(previous_data_filepath)
    prev_df = prev_df.dropna()
    clean_df = pd.read_csv(clean_data_filepath)


    merged_df = pd.concat([prev_df, clean_df], ignore_index=True)

    # 2: Save the merged DataFrame back to the previous data file
    merged_df.to_csv(previous_data_filepath, index=False)


# find last end date
def getLastEndDate(previous_data_file_path):

    df=pd.read_csv(previous_data_file_path)
    df=df.dropna()

    df['start'] = pd.to_datetime(df['start'], format='%d-%m-%Y')


    # Get the maximum (latest) date in the 'start' column
    last_date = df['start'].max().date()
    print(last_date)
    # Ensure that we have a valid date
    if not last_date:
        raise ValueError("No valid date found in the data")

    return last_date

    # return dt.strptime(last_date, '%m/%d/%Y').date()


# Paths
os.remove(r"C:\prathamesh\loop\spark_project\data\dirtyData.csv")
os.remove(r"C:\prathamesh\loop\spark_project\data\cleanData.csv")
names_file_path = r'C:\prathamesh\loop\spark_project\data\names_data.csv'
products_file_path = r'C:\prathamesh\loop\spark_project\data\Products_data.csv'
output_file_path = r'C:\prathamesh\loop\spark_project\data\dirtyData.csv'
clean_data_filepath = r'C:\prathamesh\loop\spark_project\data\cleanData.csv'
previous_data_file_path = r'C:\prathamesh\loop\spark_project\data\mergedtransactionsData.csv'
# getLastEndDate(previous_data_file_path)
# Get the last end date from previous data
last_end_date = getLastEndDate(previous_data_file_path)
today = date.today()
print(today)
if not last_end_date:
    print("No previous data found.")
elif last_end_date==today:
    print("already data upto today's date is generated")
else:

    # Generate new data from the day after last_end_date to today
    start_date = last_end_date + timedelta(days=1)
    print(start_date, today)

    makeTransactionData(start_date, today, names_file_path, products_file_path, output_file_path)

    # Clean the newly generated data
    cleanData(output_file_path, clean_data_filepath)
    mergeData(clean_data_filepath, previous_data_file_path)

