import sys
import pandas as pd

FILE_NAME_ARG = 1


def get_numbers_in_name(stock_name):
    result = []
    token = ""
    for char in stock_name:
        if char.isdigit():
            token += char          
        elif token != "":
            result.append(int(token))
            token = "" 
    if token != "":
        result.append(int(token))
    return result


def process():
    if len(sys.argv) != 2:
        print("Wrong Usage!")
        print("! Usage: python3 preprocess.py <stock csv file>")
    file_name = sys.argv[FILE_NAME_ARG]
    stock_data = pd.read_csv(file_name, encoding='utf8')
    to_remove_rows = []
    for index, row in stock_data.iterrows():
        stock_name = row['نماد']
        stock_numbers = get_numbers_in_name(stock_name)
        for number in stock_numbers:
            if number > 10:
                print(stock_name)
                to_remove_rows.append(index)
    stock_data = stock_data.drop(to_remove_rows)
    stock_data.to_csv(file_name, encoding='utf8', index=False)


process()