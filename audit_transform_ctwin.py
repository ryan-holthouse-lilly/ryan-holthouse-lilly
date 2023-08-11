"""Audit Transform CT-WIN"""
import pandas as pd

'''
Purpose: Take in a supplied filename in the form of a CT-WIN
audit trail CSV file. Data is then cleaned and parsed down
to only T_ORDER table data. This is then run through
a series of functions that reorganize the data, including
changing the old and new value columns in the data to lists
rather than strings, so that they can then be indexed by
the build_trail function.
Code Author: Ryan Holthouse, 2023
'''


# File name base for inputs.
# An alternative for this would of course be to just take
#   user input for the filename. That's actually how I was
#   originally running the file, but when testing several
#   different iterations of transformation code, this was
#   far more efficient
FILE_BASE = "26_May_to_31_May"
INFILE_NAME = FILE_BASE + ".csv"


## Try reading in the files, if it doesn't work, exit the program
try:
    baseDF = pd.read_csv(INFILE_NAME, encoding='utf-8')
except FileNotFoundError:
    print("Requested file does not exist... program exiting...")
    exit()
except Exception:
    print("Error reading in requested file... program exiting...")
    exit()


## Only saving the T_ORDER table to the new dataframe
df = baseDF.loc[baseDF.TABLENAME == 'T_ORDER']

def build_return_list():
    """Building out the list of table headers that will be utilized by the split method"""
    list_to_return = []
    list_to_return.append('ORDER_NUMBER')
    list_to_return.append('ACTUAL_DELIVERY_DATE')
    list_to_return.append('ACTUAL_SHIP_DATE')
    list_to_return.append('BACKORDER')
    list_to_return.append('CARRIER')
    list_to_return.append('CROSS_DOCK_ORDERS_FLAG')
    list_to_return.append('DEA_NUMBER')
    list_to_return.append('DELIVERY_ADDR1')
    list_to_return.append('DELIVERY_ADDR2')
    list_to_return.append('DELIVERY_ADDR3')
    list_to_return.append('DELIVERY_CITY')
    list_to_return.append('DELIVERY_COUNTRY_CODE')
    list_to_return.append('DELIVERY_COUNTRY_NAME')
    list_to_return.append('DELIVERY_NAME')
    list_to_return.append('DELIVERY_PHONE')
    list_to_return.append('DELIVERY_STATE')
    list_to_return.append('DELIVERY_ZIP')
    list_to_return.append('DESTINATION_ADDR_TYPE')
    list_to_return.append('DETAILS_FOR_RECEIVER')
    list_to_return.append('DRUG_FLAG')
    list_to_return.append('E_CTS_SHIPMENT_NUMBER')
    list_to_return.append('IVRS_NUMBER')
    list_to_return.append('IVRS_ORDER')
    list_to_return.append('ORDER_CONFIRM_DATE')
    list_to_return.append('ORDER_CREATE_USER')
    list_to_return.append('ORDER_DATE')
    list_to_return.append('ORDER_ID')
    list_to_return.append('PRIORITY')
    list_to_return.append('PRO_NUMBER')
    list_to_return.append('REFERENCE_NUMBER')
    list_to_return.append('REGION')
    list_to_return.append('REG_READY_FLAG')
    list_to_return.append('REQUESTED_DELIVERY_DATE')
    list_to_return.append('RMA_EXPIRATION_DATE')
    list_to_return.append('RMA_NUMBER')
    list_to_return.append('SHIPMENT_ADDR_TYPE')
    list_to_return.append('SHIP_EARLY_FLAG')
    list_to_return.append('SHIP_TO_ADDR1')
    list_to_return.append('SHIP_TO_ADDR2')
    list_to_return.append('SHIP_TO_ADDR3')
    list_to_return.append('SHIP_TO_CITY')
    list_to_return.append('SHIP_TO_COUNTRY_CODE')
    list_to_return.append('SHIP_TO_NAME')
    list_to_return.append('SHIP_TO_PHONE')
    list_to_return.append('SHIP_TO_STATE')
    list_to_return.append('SHIP_TO_ZIP')
    list_to_return.append('STATUS')
    list_to_return.append('TRIAL_ALIAS')
    list_to_return.append('TYPE_ID')
    list_to_return.append('WH_ID')
    list_to_return.append('OUTBOUND_DELIVERY_NUMBER')
    list_to_return.append('SHIP_TO_SAP_PLANT')
    list_to_return.append('IWRS_ORDER')
    list_to_return.append('FIT_FOR_USE')
    list_to_return.append('AOR_RECEIVED_FLAG')

    return list_to_return
headerList = build_return_list()

'''
custom_split method

Takes in a string of comma separated database columns and breaks/splits them
into a list, accounting for several cases of input failure. This does capture
all of the given data. The only case in which this method would fail is if a user
were to enter in an exact header string such as 'ORDER_NUMBER' as the data for
a given column, i.e. 'ORDER_NUMBER: REQUESTED_DELIVERY_DATE,'.

The best longterm solution would likely be to reformat the database's 
export settings so that the data would be formatted in the csv file as a list.

INPUT: row, the string of concatenated columns and their data values
RETURN: the data taken in from row, reformatted as a list item
'''
def custom_split(row):
    """See above documentation"""
    list_to_return = []
    while len(row) != 0:
        ## No colon is found, end of string/list, no more headers or data
        if row.find(":") == -1:
            break

        header_title = row[0: (row.find(":") + 1)]

        ## No comma is found, end of string/list, add rest of data to list
        if row.find(',') == -1:
            header_title = row
            list_to_return.append(header_title)
            break

        ## Removing the current column name of interest from the string
        row = row[(row.find(":") + 1):len(row)]

        ## Loops through each input case until reaching
        ## the end of the data for the given header
        while len(row) != 0:
            if row[0] == ' ':
                header_title += ' '
                row = row[1 : len(row)]

            if row.find(',') < row.find(':'):
                if (row.find(',') == -1) and (row[0 : row.find(':')]in headerList):
                    break

                if row.find(',') == -1:
                    header_title += row
                    row = ''
                    break
                else:
                    header_title += row[0 : row.find(',') + 1]
                    row = row[(row.find(',') + 1) : len(row)]

            elif (row.find(':') < row.find(',')):
                if row.find(':') == -1:
                    header_title += row
                    row = ''
                    break
                else:
                    if row[0 : row.find(':')] not in headerList:
                        header_title += row[0 : row.find(':') + 1]
                        row = row[(row.find(':') + 1) : len(row)]
                    else:
                        break
            else:
                header_title += row
                break

        list_to_return.append(header_title)

    # This segment serves to remove any unneeded extra commas
    #   or whitespace due to commas in the data
    list_to_return_no_commas = []
    for list_item in list_to_return:
        copy = list_item
        while ((copy[len(copy)-1] == ',') or (copy[len(copy)-1] == ' ')):
            copy = copy[0:len(copy)-1]
        list_to_return_no_commas.append(copy)

    return list_to_return_no_commas

## Adding new rows with list formats instead of massive strings
df['LIST_OLD'] = df['OLD_VALUES'].apply(custom_split)
df['LIST_NEW'] = df['NEW_VALUES'].apply(custom_split)

## Removing the old string based categories
df = df.drop(['OLD_VALUES','NEW_VALUES'], axis=1)

# Extracting order number into its own column
def extract_order_number(row):
    """Extracts the order number from the value list"""
    return row[0][row[0].find(':') + 2 : len(row[0])]


df['ORDER_NUMBER'] = df.iloc[:,5].apply(extract_order_number)


def extract_length(row):
    """Extracts the length of list_new columns"""
    return len(row)

df['LIST_LENGTH_OLD'] = df['LIST_OLD'].apply(extract_length)
df['LIST_LENGTH_NEW'] = df['LIST_NEW'].apply(extract_length)

'''
Any rows with a number of columns deviating from 55 are sent to a
failed data table and then dropped from the working dataframe. This
is done to prevent issues with indexing later on, while still preserving
data for manual review.

If data does 'fail', it gets exported as a separate csv file. If not, no
new file is created.

Afterwards, the failed dataframe is deleted in order to save memory.
'''
dataFailures = df.loc[(df.LIST_LENGTH_OLD != 55) | (df.LIST_LENGTH_NEW != 55)]
df = df.drop(df[df.LIST_LENGTH_OLD != 55].index)
df = df.drop(df[df.LIST_LENGTH_NEW != 55].index)

if len(dataFailures.index) != 0:
    dataFailures.to_csv("droppedData.csv", index=False)

del dataFailures

# Drops list length columns, as they're no longer necessary
df = df.drop(['LIST_LENGTH_OLD','LIST_LENGTH_NEW'], axis=1)

def reformat_deliv_loc_data(data_list):
    """Combining six aspects of delivery location into one item"""
    return_data = "DELIVERY_LOCATION_INFO: "
    return_data += data_list[7] + ", " + data_list[8] + ", "
    return_data += data_list[9] + ", " + data_list[10] + ", "
    return_data += data_list[15] + ", " + data_list[16]

    return return_data

def reformat_ship_loc_data(data_list):
    """Combining six aspects of shipment location into one item"""
    return_data = "SHIP_TO_LOCATION_INFO: "
    return_data += data_list[37] + ", " + data_list[38] + ", "
    return_data += data_list[39] + ", " + data_list[40] + ", "
    return_data += data_list[44] + ", " + data_list[45]

    return return_data

def reformat_deliv_contact_data(data_list):
    """Combining delivery name and phone number for less comparisons"""
    return_data = "DELIVERY_CONTACT_INFO: "
    return_data += data_list[13] + ", " + data_list[14]

    return return_data

def reformat_ship_contact_data(data_list):
    """Combining shipping name and phone number for less comparisons"""
    return_data = "SIHP_TO_CONTACT_INFO: "
    return_data += data_list[42] + ", " + data_list[43]

    return return_data

'''
listReformat method

This function combines some of the columns, in particular, most of
the delivery and shipping address information.

We can combine all of these so it says something like:
change in delivery location information, and in doing so save
ourselves several loops of the eventLog function per row.
'''
def reformat_list(data_list):
    """See above documentation"""
    deliv_loc_data = reformat_deliv_loc_data(data_list)
    ship_loc_data = reformat_ship_loc_data(data_list)
    deliv_cont_data = reformat_deliv_contact_data(data_list)
    ship_cont_data = reformat_ship_contact_data(data_list)


    index_drop_list = [7, 8, 9, 10, 15, 16, 37, 38, 39, 40, 44, 45, 13, 14, 42, 43]
    for index in sorted(index_drop_list, reverse=True):
        del data_list[index]
    data_list.append(deliv_loc_data)
    data_list.append(deliv_cont_data)
    data_list.append(ship_loc_data)
    data_list.append(ship_cont_data)

    return data_list

df['LIST_OLD'] = df['LIST_OLD'].apply(reformat_list)
df['LIST_NEW'] = df['LIST_NEW'].apply(reformat_list)

# Activity table dataframe
exportTRAIL = pd.DataFrame(columns=['Order_num', 'Date', 'Activity'])

'''
Build trail funtion. Takes in an entire row as input, then parses through it,
comparing indexed values for differences. If those differences exist, they are
logged into the exportTRAIL dataframe.
'''
def build_trail(row_contents):
    """Builds out the activity table using the newly formatted data"""

    # Indices to skip when building out the new audit trail
    index_skip_list = [1, 2, 15, 16]

    try:
        for index in range(len(row_contents.iloc[4])):
            if index in index_skip_list:
                continue

            old_val = row_contents.iloc[4][index]
            new_val = row_contents.iloc[5][index]

            old_val = old_val.strip()
            new_val = new_val.strip()

            if old_val != new_val:
                if index == 0:    ## Order_number
                    exportTRAIL.loc[len(exportTRAIL.index)] = [row_contents[6],row_contents[0],"Order Created"]
                    break
                elif index == 30: ## Status
                    activity = "Order status -" + new_val[new_val.find(":")+1 : len(new_val)]
                    exportTRAIL.loc[len(exportTRAIL.index)] = [row_contents[6],row_contents[0], activity]
                else:
                    activity_header = new_val[0] + new_val[1 : new_val.find(':')].lower()
                    if old_val[old_val.find(":")+1 : len(old_val)] == "":
                        activity_header += " added to order"
                    else:
                        activity_header += " updated/changed"
                    exportTRAIL.loc[len(exportTRAIL.index)] = [row_contents[6],row_contents[0], activity_header]

    except Exception:
        print("Error processing a change made on order " + row_contents.iloc[6])
        print(row_contents)

df.apply(build_trail, axis = 1)

exportMETA = pd.DataFrame(columns=['Order_num', 'Delivery_Country_Code', 'Ship_To_Country_Code', 'Delivery_Addr_Type', 'Ship_To_Addr_Type', 'Drug_Flag','Order_Date','Ship_Date','Delivery_Date','Requested_Delivery_Date','Priority','Type_Id', 'Warehouse_Id', 'Hub_Id','IWRS_Order'])

'''
hubDict dictionary item. Serves as a link between the warehouse id
values listed in the T_ORDER table and the hubs to which they belong

List provided by Valent D'Silva 
'''
hubDict = {
    "01" : "Fisher Clinical Services Inc",
    "02" : "EUROPEAN D.C.",
    "03" : "SYDNEY D.C.",
    "11" : "TORONTO D.C.",
    "12" : "MEXICO D.C.",
    "13" : "BRAZIL D.C.",
    "14" : "ARGENTINA D.C.",
    "15" : "LLY North America D.C.",
    "20" : "UKRAINE D.C.",
    "21" : "JSC IMP Logistics Russia",
    "23" : "UNITED KINGDOM D.C.",
    "30" : "SINGAPORE D.C.",
    "31" : "JAPAN D.C.",
    "32" : "INDIA D.C.",
    "33" : "CHINA-HUB D.C.",
    "34" : "CHINA-DEPOT D.C.",
}

'''
Like the build trail function, takes in a full row and parses through the data,
saving it instead to a metadata table frame.

If an error arises, row number is skipped and printed to the console.
'''
def build_meta(row_contents):
    """Builds out the metadata table using the newly formatted data"""
    # row_contents.iloc[0] - Timestamp
    # row_contents.iloc[1] - User
    # row_contents.iloc[2] - Table
    # row_contents.iloc[3] - Action
    # row_contents.iloc[4] - List_Old
    # row_contents.iloc[5] - List_New
    # row_contents.iloc[5][index] - List_New[index]
    # row_contents.iloc[6] - OrderNumber

    try:
        order = row_contents.iloc[5][0][row_contents.iloc[5][0].find(":") + 2: len(row_contents.iloc[5][0])]
        deliv_code = row_contents.iloc[5][7][row_contents.iloc[5][7].find(":") + 2: len(row_contents.iloc[5][7])]
        ship_code = row_contents.iloc[5][29][row_contents.iloc[5][29].find(":") + 2: len(row_contents.iloc[5][29])]

        deliv_type = row_contents.iloc[5][9][row_contents.iloc[5][9].find(":") + 2: len(row_contents.iloc[5][9])]
        ship_type = row_contents.iloc[5][27][row_contents.iloc[5][27].find(":") + 2: len(row_contents.iloc[5][27])]
        drug_flag = row_contents.iloc[5][11][row_contents.iloc[5][11].find(":") + 2: len(row_contents.iloc[5][11])]

        order_date = row_contents.iloc[5][17][row_contents.iloc[5][17].find(":") + 2: len(row_contents.iloc[5][17])]
        ship_date = row_contents.iloc[5][2][row_contents.iloc[5][2].find(":") + 2: len(row_contents.iloc[5][2])]
        deliv_date = row_contents.iloc[5][1][row_contents.iloc[5][1].find(":") + 2: len(row_contents.iloc[5][1])]

        req_deliv_date = row_contents.iloc[5][24][row_contents.iloc[5][24].find(":") + 2: len(row_contents.iloc[5][24])]
        prior = row_contents.iloc[5][19][row_contents.iloc[5][19].find(":") + 2: len(row_contents.iloc[5][19])]
        type_id = row_contents.iloc[5][32][row_contents.iloc[5][32].find(":") + 2: len(row_contents.iloc[5][32])]

        wh_id = row_contents.iloc[5][33][row_contents.iloc[5][33].find(":") + 2: len(row_contents.iloc[5][33])]
        hub = hubDict[wh_id]
        iwrs = row_contents.iloc[5][36][row_contents.iloc[5][36].find(":") + 2: len(row_contents.iloc[5][36])]

        exportMETA.loc[len(exportMETA.index)] = [order, deliv_code, ship_code, deliv_type, \
                                                 ship_type, drug_flag, order_date, ship_date, \
                                                    deliv_date, req_deliv_date, prior, \
                                                        type_id, wh_id, hub, iwrs]
    except Exception:
        print("Error processing the meta log for order " + row_contents.iloc[6])
        print(row_contents)

df = df.drop_duplicates('ORDER_NUMBER', keep='last')
df = df.sort_values('ORDER_NUMBER')

df.apply(build_meta, axis = 1)

# Finally, export the files that were created
exportTRAIL.to_csv(FILE_BASE + "_TEST_TRAIL.csv", index=False)
exportMETA.to_csv(FILE_BASE + "__TEST_META.csv", index=False)
