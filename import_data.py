import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import sys
import os
import logging

# change these
user = "root"
password = ""
host = "localhost"
db = "coral_test"
directory_name = "Photo_data"
# these should all be lowercase
taxon_columns_names_list = ['taxon', 'phylum', 'kingdom', 'class', 'order', 'family', 'genus', 'species']
photo_id_column_names = ['photo', 'id']
table_name = 'Photo_Table'

logger = logging.getLogger()


def iterate_through_files():
    # TODO: remove later, redirect output to a file
    import sys
    orig_stdout = sys.stdout
    f = open('sheets.txt', 'w')
    sys.stdout = f

    # connect to mysql
    engine = create_engine("mysql://{}:{}@{}/{}".format(user, password, host, db))
    con = engine.connect()
    dir = os.getcwd() + "/" + directory_name
    for filename in os.listdir(dir):
        if (filename.endswith(".xlsx") or filename.endswith(".xls")) and not filename.startswith('~$'):
            # check for excel file extension
            # for filename in files
            filepath = dir + "/" + filename
            sheet_dict = read_in_excel_sheets(filepath)
            process_sheets(filename, sheet_dict)

    # end of for loop
    con.close()

    # end of redirected output
    sys.stdout = orig_stdout
    f.close()


def process_sheets(filename, sheet_dict):
    new_sheet_dict = sheet_dict.copy()
    # TODO: add logging
    for key in sheet_dict.keys():  # Doesn't work right
        sheet_df = sheet_dict[key]
        sheet_df.dropna(axis=1, how='all')
        sheet_df.columns = map(str.lower, sheet_df.columns.astype(str))
        photo_columns = []
        taxon_columns = []
        for column in sheet_df.columns:
            if column in taxon_columns_names_list:
                taxon_columns.append(column)
            elif column in photo_id_column_names:
                photo_columns.append(column)
            else:
                for taxon in taxon_columns_names_list:
                    if taxon in column:
                        taxon_columns.append(column)
                # right now this is only a few elements
                for id in photo_id_column_names:
                    if id in column:
                        photo_columns.append(column)

        if len(photo_columns) == 0 or len(taxon_columns) == 0:
            logger.info("Deleted file {}, sheet {}".format(filename, key))
            del new_sheet_dict[key]
        else:
            # Find a column that contains either 'photo' or 'id' in the column name with the least amount of null values
            photo_column = (sheet_df[photo_columns].isnull().sum()).idxmin()
            taxon_columns.append(photo_column)
            new_sheet_dict[key] = sheet_df[taxon_columns]
            print("Before deletion:")
            print(new_sheet_dict[key].isnull().sum())
            print("After Deletion:")
            print(new_sheet_dict[key].isnull().sum())

            if new_sheet_dict[key].empty:
                logger.info("Deleted file {}, sheet {}".format(filename, key))
                del new_sheet_dict[key]

            # TODO: delete later, debug function
            # print_debug(new_sheet_dict, filename)


def isolate_important_columns(sheet_df):


def build_primary_key(photo_column, taxon_columns, sheet_df):
    print(taxon_columns)


def print_debug(new_sheet_dict, filename):
    for key in new_sheet_dict.keys():
        df = new_sheet_dict[key]
        print("Reading in: {}_{}".format(filename, key))
        print(df.head())
        print(df.columns)
        print(df.isnull().sum())
        print(df.columns)
        print(df.isnull().sum())
        print()


# def convert_xlsx_to_tsv():
#     df = pd.read_excel("/Users/lehcar/Documents/Spring 2020/CS 180H/coral_data_python/Photo_data/2008.Mayotte.xls")
#     print(df.head())
#     df.to_csv('fileTSV.tsv', sep='\t', encoding='utf-8',  index=False, line_terminator='\r\n')
#     return df

# TODO: fix this
def read_in_args():
    if len(sys.argv) is not 3:
        print("ERROR")


def read_in_excel_sheets(filepath):
    sheet_dict = pd.read_excel(filepath, sheet_name=None, dtype=str)
    return sheet_dict


def read_in_excel(filepath):
    df = pd.read_excel(filepath)
    # process dataframe
    # set primary key'
    return df


def create_table():
    conn = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
        database=db
    )

    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))

    cursor.execute("CREATE TABLE {} (taxon VARCHAR(255) PRIMARY KEY, photo_ids LONGTEXT, row_num INT)".format(table_name))

    conn.close()


def send_to_mysql(df, filename, con):
    filename = filename.replace('.', '_')
    df.to_sql(name=filename, con=con, if_exists='append')


if __name__ == '__main__':
    create_table()
    iterate_through_files()
    # df = convert_xlsx_to_tsv()
    # send_to_mysql(df, '2008.Mayotte.xls', con)
    # con.close()

# Primary key
# P_PhylumName__F_FamilyName
# P_PhylumName__G_GenusName

# Photos
# array of photo files

# dFar.xls
# rows 238-240: ignore the notes after the semi-colon
# Taxon column: 2 words, first is Genus, then species
# ignore files without genus, species, phylum, or taxon column

# Maybe a separate table with photo ID as the primary key?
# Columns naming the original spreadsheet and row number (for the first one)


# NOTES: added a header to second sheet of dAUST1.xlsx

# Ask about ArtMadaPhotos.xls sheet 3
