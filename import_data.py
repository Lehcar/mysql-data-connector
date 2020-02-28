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
photo_id_column_names = ['photo', 'id']  # all elements should be lowercase
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

        photo_column, target_columns = isolate_important_columns(sheet_df)

        if photo_column == None or target_columns == None:
            logger.info("Deleted file {}, sheet {}".format(filename, key))
            del new_sheet_dict[key]
        else:
            print("Photo column: {}".format(photo_column))
            print(target_columns)
        #     # Find a column that contains either 'photo' or 'id' in the column name with the least amount of null values
        #     photo_column = (sheet_df[photo_columns].isnull().sum()).idxmin()
        #     new_sheet_dict[key] = add_primary_key(photo_column, taxon_columns, new_sheet_dict[key])
        #     print("Before deletion:")
        #     print(new_sheet_dict[key].isnull().sum())
        #     print("After Deletion:")
        #     print(new_sheet_dict[key].isnull().sum())
        #
        #     if new_sheet_dict[key].empty:
        #         logger.info("Deleted file {}, sheet {}".format(filename, key))
        #         del new_sheet_dict[key]
        #
        #     # TODO: delete later, debug function
        #     # print_debug(new_sheet_dict, filename)


def isolate_important_columns(sheet_df):
    photo_columns = []
    taxon_columns = []
    kingdom_columns = []
    phylumn_columns = []
    class_columns = []
    order_columns = []
    family_columns = []
    genus_columns = []
    species_columns = []
    target_columns = []

    for column in sheet_df.columns:
        # FIXME: Kinda hacky, maybe a switch statement would be better?
        # TODO: see if there's a case where there's a column called GenusSpecies or some nonsense <_<
        # otherwise change to elif statements
        if 'taxon' in column:
            taxon_columns.append(column)
        if 'kingdom' in column:
            kingdom_columns.append(column)
        if 'phylumn' in column:
            phylumn_columns.append(column)
        if 'class' in column:
            class_columns.append(column)
        if 'order' in column:
            order_columns.append(column)
        if 'family' in column:
            family_columns.append(column)
        if 'genus' in column:
            genus_columns.append(column)
        if 'species' in column:
            species_columns.append(column)

        # right now this is only a few elements
        for id in photo_id_column_names:
            if id in column:
                photo_columns.append(column)
    if len(photo_columns) == 0:
        return None, None
    # FIXME: also hacky, might be my best option but gotta explore other options
    target_columns.append(taxon_columns)
    append_columns_helper(target_columns, kingdom_columns, sheet_df)
    append_columns_helper(target_columns, phylumn_columns, sheet_df)
    append_columns_helper(target_columns, class_columns, sheet_df)
    append_columns_helper(target_columns, order_columns, sheet_df)
    append_columns_helper(target_columns, family_columns, sheet_df)
    append_columns_helper(target_columns, genus_columns, sheet_df)
    append_columns_helper(target_columns, species_columns, sheet_df)

    return sheet_df[photo_columns].isnull().sum().idxmin(), target_columns


def append_columns_helper(master_list, column_list, sheet_df):
    if len(column_list) == 0:
        master_list.append("")
    else:
        master_list.append(sheet_df[column_list].isnull().sum().idxmin())


def add_primary_key(photo_column, taxon_columns, sheet_df):
    """
    Builds primary key: Kingdom, phylum, class, order, family, genus, species
     - e.g. K_kingdomName__P_phylumName__C_className__O_orderName__F_familyName__G_genusName__S_speciesName

    :param photo_column:
    :param taxon_columns:
    :param sheet_df:
    :return:
    """
    taxon_columns.append(photo_column)
    sheet_df = sheet_df[taxon_columns]
    return sheet_df


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
