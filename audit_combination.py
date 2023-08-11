"""Audit Combination"""
import pandas as pd

'''
Purpose: Combine together several supplied audit trail
and metadata table files into two new files, a full trail
and full metadata table.
Code Author: Ryan Holthouse, 2023
'''

# Reading in several trail and dataframe files. I would have
#   liked to have set this up to loop through and take in multiple
#   user supplied inputs, but it ended up being run in this way
#   due to the urgency of testing the values and supplying the
#   data to Celonis.
trailDF1 = pd.read_csv('16_May_to_20_May_TRAIL.csv', encoding='utf-8')
trailDF2 = pd.read_csv('21_May_to_25_May_TRAIL.csv', encoding='utf-8')
trailDF3 = pd.read_csv('26_May_to_31_May_TRAIL.csv', encoding='utf-8')


metaDF1 = pd.read_csv('16_May_to_20_May_META.csv', encoding='utf-8')
metaDF2 = pd.read_csv('21_May_to_25_May_META.csv', encoding='utf-8')
metaDF3 = pd.read_csv('26_May_to_31_May_META.csv', encoding='utf-8')

# Concatenating all of the supplied trail information into one frame, then
#   sorting the values by date.
full_trail_df = pd.concat([trailDF1, trailDF2, trailDF3], ignore_index=True)
full_trail_df = full_trail_df.sort_values('Date')


# Concatenating all of the supplied metadata information into one frame, then
#   sorting the values by date. After, all duplicate order records are dropped
#   and only the last is kept, which will represent the most up-to-date
#   information for the given order.
full_meta_df = pd.concat([metaDF1, metaDF2, metaDF3], ignore_index=True)
full_meta_df = full_meta_df.sort_values('Order_num').drop_duplicates('Order_num', keep='last')

month_dict = {
    "JAN" : "01",
    "FEB" : "02",
    "MAR" : "03",
    "APR" : "04",
    "MAY" : "05",
    "JUN" : "06",
    "JUL" : "07",
    "AUG" : "08",
    "SEP" : "09",
    "OCT" : "10",
    "NOV" : "11",
    "DEC" : "12",
}

def transform_dates_timed(row):
    '''
    Transforming dates into a timed format to act as the audit trail
    timestamp.
    INPUT: row, the data from the supplied column for an applied record/row
    RETURN: A modified timestamp string.
    '''
    ## row.iloc[1] = Date
    date_str = row

    day = date_str[0 : date_str.find('-')]
    date_str = date_str[date_str.find('-') + 1: len(date_str)]
    month = date_str[0 : date_str.find('-')]
    date_str = date_str[date_str.find('-') + 1: len(date_str)]
    year = date_str

    if len(month) > 2:
        month = month_dict[month]

    if len(date_str) == 2:
        year = "20" + year

    timestamp_str = year + '-' + month + '-' + day + ' ' + "14:00:00"
    return timestamp_str

def transform_dates_untimed(row):
    '''
    Transforming dates that don't have/need any particular time assignment
    INPUT: row, the data from the supplied column for an applied record/row
    RETURN: Either an unmodified data object if the value is null, or a
    reformatted timestamp if it's not.
    '''
    ## row.iloc[1] = Date
    if isinstance(row, float):
        return row
    else:
        date_str = row

        day = date_str[0 : date_str.find('-')]
        date_str = date_str[date_str.find('-') + 1: len(date_str)]
        month = date_str[0 : date_str.find('-')]
        date_str = date_str[date_str.find('-') + 1: len(date_str)]
        year = date_str

        if len(month) > 2:
            month = month_dict[month]

        if len(date_str) == 2:
            year = "20" + year

        timestamp_str = year + '-' + month + '-' + day
        return timestamp_str

#Running transformations on trail data to ensure proper date formatting
full_trail_df['Date'] = full_trail_df['Date'].apply(transform_dates_timed)

#Running transformations on various metadata table columns to ensure proper formatting
full_meta_df['Delivery_Date'] = full_meta_df['Delivery_Date'].apply(transform_dates_untimed)
full_meta_df['Order_Date'] = full_meta_df['Order_Date'].apply(transform_dates_untimed)
full_meta_df['Ship_Date'] = full_meta_df['Ship_Date'].apply(transform_dates_untimed)
full_meta_df['Requested_Delivery_Date'] = full_meta_df['Requested_Delivery_Date'].apply(transform_dates_untimed)

#CSV outputs in order to check that everything is outputting correctly
#full_trail_df.to_csv("fullTrailCheck.csv", index=False)
#full_meta_df.to_csv("fullMetaCheck.csv", index=False)

#Outputting to Parquet for full export
full_trail_df.to_parquet("fullTrail.parquet", index=False)
full_meta_df.to_parquet("fullMeta.parquet", index=False)
