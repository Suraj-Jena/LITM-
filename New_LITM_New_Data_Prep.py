# -*- coding: utf-8 -*-
"""
Created on Thu Jan  4 11:26:11 2018

@author: saad.syed
"""

import os
import glob
import json
from flatten_json import flatten
import pandas as pd
from collections import defaultdict


# GENERATOR FUNCTION TO READ JSON FILES SATISFYING THE FILE NAME PATTERN FROM GIVEN PATH,
# CAN OPTIONALLY SPECIFY THE ENCODING
# read_jsons() RETURNS A DICT

def read_jsons(path, file_pattern, encoding='utf8'):
    os.chdir(path)
    for filename in glob.iglob(file_pattern, recursive=True):
        try:
            with open(filename, encoding=encoding) as json_data:
                yield json.load(json_data), filename
        except Exception as e:
            print(str(e), filename)


def check_data(data):
    if 'checkNumber' not in list(data['joasisMLCheckContext'].keys()):
        return True
    elif data['joasisMLCheckContext']['checkNumber'] is "":
        return True
    return False


def create_row_string(merged_word_context_list):
    merged_string = ' '.join([merged_word['value'].replace('\r', '') for merged_word in merged_word_context_list])
    return merged_string


def prefix_dict(old_dict, prefix):
    new_dict = {prefix + key: old_dict[key] for key in old_dict.keys()}
    return new_dict


def get_section_dict(section_context_list, row_no):
    cum_rows = 0
    for section in section_context_list:
        cum_rows += section['noOfRows']
        if row_no <= cum_rows:
            return section


def get_row_dict(row):
    row_dict = defaultdict()
    row_dict = {key: row[key] for key in row.keys() if key != 'joasisMLMergedWordContexts'}
    row_dict['string'] = create_row_string(row['joasisMLMergedWordContexts'])
    row_dict['rowNumber'] = row_dict['rowNumber'] - 1
    row_dict.update(get_tag_count(row['joasisMLMergedWordContexts']))
    return flatten_prefix(row_dict, 'row_')


# Prefix then Flatten
def flatten_prefix(dictionary, prefix):
    return flatten(prefix_dict(dictionary, prefix))


# Get word tag counts
def get_tag_count(merged_word_list):
    tag_count_dict = defaultdict(int)
    for merged_word in merged_word_list:
        tag_count_dict[merged_word['tag']] += 1

    return tag_count_dict


def create_df(path, file_pattern):
    data_generator = read_jsons(path, file_pattern)
    final_dict_list = []
    for data, filename in data_generator:
        if check_data(data):
            print("No check number")
            continue
        for row in data['joasisMLRowContexts']:
            final_dict = defaultdict()
            row_dict = get_row_dict(row)
            section_dict = get_section_dict(data['joasisMLSectionContexts'], row['rowNumber'])

            final_dict.update(row_dict)
            final_dict.update(flatten_prefix(section_dict, 'section_'))
            final_dict.update(flatten_prefix(data['joasisMLCheckContext'], 'check_'))
            final_dict.update(flatten_prefix(data['joasisMLPageContext'], 'page_'))
            final_dict['check_checkNumber'] = float(final_dict['check_checkNumber'])
            final_dict['file'] = filename
            final_dict_list.append(final_dict)

    all_rows = pd.DataFrame(final_dict_list)
    return all_rows


jsons_path = r'E:\LITM\LITM Data\Modspace\New' 
file_pattern = '**/ML/*.json'
all_rows = create_df(jsons_path, file_pattern)
# Item level status file
item_status_file_path = r'E:\LITM\LITM Data\Modspace\New\CSVs\Check_item_status.csv'
item_status = pd.read_csv(item_status_file_path, sep=',',
                          usecols=['check_number', 'initial_status', 'indexing_status', 'is_deleted', 'page_number',
                                   'row_number'])

item_status['row_number'] = item_status['row_number'].astype(int)
item_status['page_number'] = item_status['page_number'].astype(int)
item_status['check_number'] = item_status['check_number'].astype(float)

# Check Level Status file
check_status_file_path = r'E:\LITM\LITM Data\Modspace\New\CSVs\Batch_check_status.csv'
check_status = pd.read_csv(check_status_file_path, sep='\t')

# Add check status for each row
status_map_dict = dict(zip(check_status['check_number'], check_status['indexing_status']))
all_rows['check_status'] = all_rows['check_checkNumber'].map(status_map_dict)

# Add batch name for each row
batch_map_dict = dict(zip(check_status['check_number'], check_status['batch_name']))
all_rows['batch_name'] = all_rows['check_checkNumber'].map(batch_map_dict)

# Add batch id for each row
batch_id_map_dict = dict(zip(check_status['check_number'], check_status['batch_id']))
all_rows['batch_id'] = all_rows['check_checkNumber'].map(batch_id_map_dict)

item_status = item_status[~(item_status['row_number'].isnull() | item_status['page_number'].isnull())]
item_status = item_status[item_status['is_deleted'] == 0]

all_rows['unique_row_identifier'] = all_rows['check_checkNumber'].astype(str) + "-" + all_rows[
    'page_pageNumber'].astype(str) + "-" + all_rows['row_rowNumber'].astype(str)
item_status['unique_row_identifier'] = item_status['check_number'].astype(str) + "-" + item_status[
    'page_number'].astype(str) + "-" + item_status['row_number'].astype(str)

# Add item status for each row
item_status['is_remittance_final'] = 1
item_map_dict = dict(zip(item_status['unique_row_identifier'], item_status['is_remittance_final']))
all_rows['is_remittance_final'] = all_rows['unique_row_identifier'].map(item_map_dict)
all_rows['is_remittance_final'].fillna(0, inplace=True)

all_rows['image_file'] = all_rows['file'].apply(lambda x: x[:x.find('ML')]).astype(str) + "input\\" + all_rows[
    'page_pageNumber'].astype(str) + ".tif"

final_file_path = r'E:\LITM\LITM Data\Modspace\New\CSVs\Modspace_new.csv'
all_rows.to_csv(final_file_path, index=False)
