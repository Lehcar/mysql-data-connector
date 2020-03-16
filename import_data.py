import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import sys
import os
import time
import math

# change these
user = ''
password = ''
host = "localhost"
db = ''
directory_name = "Photo_data"
photo_id_column_names = ['photo', 'id']  # all elements should be lowercase
table_name = ''
PRIMARY_KEY_COL_NAME = 'taxon_key'  # note: this should not be taxon


# TODO: test this with an actual password instead of null
def read_in_args():
    # assumes program runs with 'python script_name.py username password db_name table_name'
    if len(sys.argv) is not 5:
        raise Exception('must include username, password, db_name, and table_name as command line arguments')

    global user, password, db, table_name
    user = sys.argv[1]
    password = sys.argv[2]
    if str.lower(password) == 'null':
        password = ''
    db = sys.argv[3]
    table_name = sys.argv[4]


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

    # TODO: remove redirect output code later
    # end of redirected output
    sys.stdout = orig_stdout
    f.close()


def process_sheets(filename, sheet_dict):
    new_sheet_dict = sheet_dict.copy()

    for key in sheet_dict.keys():  # Doesn't work right
        sheet_df = sheet_dict[key]
        sheet_df.dropna(axis=1, how='all')
        sheet_df.columns = map(str.lower, sheet_df.columns.astype(str))

        photo_column, target_columns = isolate_important_columns(sheet_df)

        if photo_column is None or target_columns is None:
            del new_sheet_dict[key]
        else:
            build_primary_key(photo_column, target_columns, sheet_df)
        #
        #     if new_sheet_dict[key].empty:
        #         del new_sheet_dict[key]
        #
        #     # TODO: delete later, debug function
        #     # print_debug(new_sheet_dict, filename)


def isolate_important_columns(sheet_df):
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
    if len(column_list) == 0:
        master_list.append('')
    else:
        master_list.append(sheet_df[column_list].isnull().sum().idxmin())


def build_primary_key(photo_column, target_columns, sheet_df):
    """
    Builds primary key: Kingdom, phylum, class, order, family, genus, species
         - e.g. K_kingdomName__P_phylumame__C_className__O_orderName__F_familyName__G_genusName__S_speciesName

        :param photo_column:
        :param taxon_columns:
        :param sheet_df:
        :return:
    """
    # FIXME: split the taxon name!!!!!!!!!!!! ;_____________________;
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
    print('Key column: ')
    print(sheet_df[PRIMARY_KEY_COL_NAME])
    print()

    # delete blank primary keys (e.g. x is not 'K__P__C__O__F__G__S__')


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


def convert_excel_to_tsv(excel_filepath, new_tsv_filepath):
    """
        Helper function to convert excel files to tsv files.
        INFO: not currently used in the program, but left in for potential future usage

        :param excel_filepath: filepath to the xls or xlsx file (excel file)
        :param new_tsv_filepath: filepath where the new .tsv file will be written to
        :return: returns a dataframe containing the contents of the excel file
    """
    df = pd.read_excel(excel_filepath)
    print(df.head())
    df.to_csv(new_tsv_filepath, sep='\t', encoding='utf-8', index=False, line_terminator='\r\n')
    return df


def read_in_excel_sheets(filepath):
    sheet_dict = pd.read_excel(filepath, sheet_name=None, dtype=str)
    return sheet_dict


def create_table():
    conn = mysql.connector.connect(
        host=host,
        user=user,
        passwd=password,
        database=db
    )

    cursor = conn.cursor()

    # # FIXME: instead of dropping if it exists, check it if it exists before creating
    # cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))
    sql = "CREATE TABLE IF NOT EXISTS {} ({} VARCHAR(255) PRIMARY KEY, taxon VARCHAR(255), photo_ids LONGTEXT, " \
          "row_num INT);".format(table_name, PRIMARY_KEY_COL_NAME)

    print(sql)

    cursor.execute(sql)

    conn.close()


def send_to_mysql(df, filename, con):
    filename = filename.replace('.', '_')
    df.to_sql(name=filename, con=con, if_exists='append')


if __name__ == '__main__':
    start = time.clock()  # FIXME: depricated in python 3.8, find alternative
    read_in_args()
    create_table()
    iterate_through_files()
    print("Total Runtime (in seconds): {}".format(time.clock() - start))
    # df = convert_xlsx_to_tsv()
    # send_to_mysql(df, '2008.Mayotte.xls', con)
    # con.close()

# Primary key
# P_phylumame__F_FamilyName
# P_phylumame__G_GenusName

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
