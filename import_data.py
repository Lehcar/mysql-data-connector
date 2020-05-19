import os
import time
import sys

import mysql.connector
from sqlalchemy import create_engine
import pandas as pd

user = ''
password = ''
host = "localhost"
db = ''
directory_name = "Photo_data"
photo_id_column_names = ['photo', 'id']  # all elements should be lowercase
table_name = ''
PRIMARY_KEY_COL_NAME = 'taxon_key'  # note: this should not be taxon
conn = None


def read_in_args():
    """
    Sets mysql user, password, database, and table_name based on command line arguments.
    Assumes program runs with 'python script_name.py username password db_name table_name' command.
    Raises an exception if there is a missing argument.

    """
    if len(sys.argv) is not 5:
        raise Exception('Must include username, password, db_name, and table_name as command line arguments')

    global user, password, db, table_name
    user = sys.argv[1]
    password = sys.argv[2]
    if str.lower(password) == 'null':
        password = ''
    db = sys.argv[3]
    table_name = sys.argv[4]


def iterate_through_files():
    """

    """
    # connect to mysql
    engine = create_engine("mysql://{}:{}@{}/{}".format(user, password, host, db))
    con = engine.connect()
    dir = os.getcwd() + "/" + directory_name
    total_num_excel_files = 0
    for filename in os.listdir(dir):
        if (filename.endswith(".xlsx") or filename.endswith(".xls")) and not filename.startswith('~$'):
            # check for excel file extension
            # for filename in files
            filepath = dir + "/" + filename
            sheet_dict = read_in_excel_sheets(filepath)
            process_sheets(filename, sheet_dict)
            total_num_excel_files += 1

    print('{} excel files processed'.format(total_num_excel_files))
    # end of for loop
    con.close()


def process_sheets(filename, sheet_dict):
    """

    :param filename:
    :param sheet_dict:
    """
    new_sheet_dict = sheet_dict.copy()

    for key in sheet_dict.keys():  # Doesn't work right
        sheet_df = sheet_dict[key]
        sheet_df.dropna(axis=1, how='all')
        sheet_df.columns = map(str.lower, sheet_df.columns.astype(str))

        photo_column, target_columns = isolate_important_columns(sheet_df)

        if photo_column is None or target_columns is None:
            del new_sheet_dict[key]
            continue

        sheet_df = build_primary_key(photo_column, target_columns, sheet_df)

        get_subset(filename, sheet_df, target_columns[0], photo_column)


def isolate_important_columns(sheet_df):
    """

    :param sheet_df:
    :return:
    """
    photo_columns = []
    taxon_columns = []
    kingdom_columns = []
    phylum_columns = []
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
        if 'phylum' in column:
            phylum_columns.append(column)
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
    if len(photo_columns) == 0 or (len(taxon_columns) == 0 and len(kingdom_columns) == 0 and len(phylum_columns) == 0
                                   and len(class_columns) == 0 and len(order_columns) == 0 and
                                   len(family_columns) == 0 and len(genus_columns) == 0
                                   and len(species_columns) == 0):
        return None, None

    # FIXME: also hacky, might be my best option but gotta explore other options
    append_columns_helper(target_columns, taxon_columns, sheet_df)
    append_columns_helper(target_columns, kingdom_columns, sheet_df)
    append_columns_helper(target_columns, phylum_columns, sheet_df)
    append_columns_helper(target_columns, class_columns, sheet_df)
    append_columns_helper(target_columns, order_columns, sheet_df)
    append_columns_helper(target_columns, family_columns, sheet_df)
    append_columns_helper(target_columns, genus_columns, sheet_df)
    append_columns_helper(target_columns, species_columns, sheet_df)

    return sheet_df[photo_columns].isnull().sum().idxmin(), target_columns


def append_columns_helper(master_list, column_list, sheet_df):
    """

    :param master_list:
    :param column_list:
    :param sheet_df:
    :return:
    """
    if len(column_list) == 0:
        master_list.append('')
    else:
        master_list.append(sheet_df[column_list].isnull().sum().idxmin())


def build_primary_key(photo_column, target_columns, sheet_df):
    """
    Builds primary key: Kingdom, phylum, class, order, family, genus, species
         - e.g. K_kingdomName__P_phylumName__C_className__O_orderName__F_familyName__G_genusName__S_speciesName__

    :param photo_column:
    :param taxon_columns:
    :param sheet_df:
    :return:
    """
    prefixes = ['T', 'K', 'P', 'C', 'O', 'F', 'G', 'S']
    sheet_df[PRIMARY_KEY_COL_NAME] = ''  # sets the column to a blank string as the default value
    for i in range(1, len(prefixes)):
        if target_columns[i] is not '':
            curr_column = list(sheet_df[target_columns[i]])
            keys_column = list(sheet_df[PRIMARY_KEY_COL_NAME])
            for j in range(0, len(curr_column)):
                if type(curr_column[j]) is not float:  # null values show up as 'nan'
                    value = str(curr_column[j])
                    keys_column[j] = keys_column[j] + prefixes[i] + '_' + value + '_'
                else:
                    keys_column[j] = keys_column[j] + prefixes[i] + '__'
            sheet_df[PRIMARY_KEY_COL_NAME] = keys_column
        else:
            sheet_df[PRIMARY_KEY_COL_NAME] = sheet_df[PRIMARY_KEY_COL_NAME].apply(lambda x: x + prefixes[i] + '__')

    # delete blank primary keys (e.g. x is not 'K__P__C__O__F__G__S__') and keys without a taxon column value
    # if there is no 'taxon' column:
    taxon_column = target_columns[0]
    if taxon_column is '':
        sheet_df = sheet_df[sheet_df[PRIMARY_KEY_COL_NAME] != 'K__P__C__O__F__G__S__']
        return sheet_df

    # if there's a taxon column, only delete rows with no key and no value for taxon
    sheet_df = sheet_df[(sheet_df[PRIMARY_KEY_COL_NAME] != 'K__P__C__O__F__G__S__') &
                        (sheet_df[target_columns[0]] != None)]

    return sheet_df


def get_subset(filename, sheet_df, taxon_column, photo_id_column):
    new_df = pd.DataFrame()
    new_df[PRIMARY_KEY_COL_NAME] = sheet_df[PRIMARY_KEY_COL_NAME]
    if taxon_column is '':
        new_df['taxon'] = None
    else:
        new_df['taxon'] = sheet_df[taxon_column]
    new_df['photo_id'] = sheet_df[photo_id_column]
    new_df['filename'] = filename
    new_df.drop_duplicates(subset='photo_id', keep='first', inplace=True)
    send_to_mysql(new_df, 'table_with_photo_id')


def print_debug(df):
    print(df.head())
    print("INDEX: {}".format(df.index))
    print("COLUMNS: {}".format(df.columns))
    print("NULL COUNTS: {}".format(df.isnull().sum()))
    print(df.dtypes)
    print()


def convert_excel_to_tsv(excel_filepath, new_tsv_filepath):
    """
        Helper function to convert excel files to tsv files.
        INFO: not currently used in the program, but left in for potential future usage

        :param excel_filepath: filepath to the xls or xlsx file (excel file)
        :param new_tsv_filepath: filepath where the new .tsv file will be written to
        :return: returns a dataframe containing the contents of the excel file
    """
    df = pd.read_excel(excel_filepath)
    df.to_csv(new_tsv_filepath, sep='\t', encoding='utf-8', index=False, line_terminator='\r\n')
    return df


def read_in_excel_sheets(filepath):
    sheet_dict = pd.read_excel(filepath, sheet_name=None, dtype=str)
    return sheet_dict


def create_table():
    global conn
    conn = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
        database=db
    )

    cursor = conn.cursor()

    sql = "DROP TABLE IF EXISTS {}".format(table_name)

    cursor.execute(sql)

    sql = "CREATE TABLE IF NOT EXISTS {} ({} VARCHAR(255), " \
          "photo_id LONGTEXT, filename LONGTEXT);".format(table_name, PRIMARY_KEY_COL_NAME)

    cursor.execute(sql)

    # FIXME: unable to have photo_id as index due to duplicate values
    sql = "CREATE TABLE IF NOT EXISTS main_table(photo_id VARCHAR(255), taxon_key VARCHAR(255), " \
          "taxon VARCHAR(255), filename VARCHAR(255));"

    cursor.execute(sql)
    conn.close()


def send_to_mysql(df, name):
    db_con = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format(user, password, host, db))
    df.to_sql(con=db_con, name=name, if_exists='append', index=False)


def create_primary_key_table():
    df = read_in_mysql()
    df = process_data_frame(df)
    df.to_csv('table_with_key.tsv', sep='\t', encoding='utf-8', index=True, line_terminator='\r\n')
    df.to_csv('table_with_key.csv', index=True)
    db_con = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format(user, password, host, db))
    df.to_sql(con=db_con, name=table_name, if_exists='append', index=True)


def read_in_mysql():
    db_con = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format(user, password, host, db)).connect()
    df = pd.read_sql("select * from table_with_photo_id", db_con)
    df.drop_duplicates(keep='first', inplace=True)
    db_con.close()
    df.to_csv('table_with_photo_id.tsv', sep='\t', encoding='utf-8', index=False, line_terminator='\r\n')
    df.to_csv('table_with_photo_id.csv', index=False)
    return df


def process_data_frame(df):
    if 'taxon' in df.columns:
        df.drop(['taxon'], inplace=True, axis=1)
    df = df.groupby(PRIMARY_KEY_COL_NAME, as_index=True).aggregate(lambda x: '; '.join(map(str, x)))
    return df


if __name__ == '__main__':
    start = time.clock()  # FIXME: depricated in python 3.8, find alternative

    read_in_args()
    create_table()
    iterate_through_files()
    create_primary_key_table()

    print("Total Runtime (in seconds): {}".format(time.clock() - start))

