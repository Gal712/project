
import pandas as pd
from pandas.io.json import json_normalize
import urllib.request
import json
from tqdm.notebook import tqdm as tqdm
import codecs
import time
from IPython.display import display
import matplotlib.pyplot as plt
import io
import numpy as np
import itertools
import six

base_url = "https://sys.archives.gov.il/api/solr?fq[]=lang_code_s:he&fq[]=product_id_i:"
addURL = "https://storage.archives.gov.il/Archives/"
base_attachment_url = "https://www.archives.gov.il/"
product_extra_meta_data_url = "https://storage.archives.gov.il/Archives/{0}/Files/{1}/FILE-{2}.jsn"
product_data_url = "https://sys.archives.gov.il/api/solr?rows=1&fq[]=product_code_s:{0}&fq[]=lang_code_s:he"

download_metadata_dir = "C:/Users/Adi/Desktop/Yael/Project/MetaDataDir/"
download_extra_metadata_dir = "C:/Users/Adi/Desktop/Yael/Project/ExtraMetaDataDir/"
download_attachment_dir = "C:/Users/Adi/Desktop/Yael/Project/AttachmentDir/"

start_year_to_filter = 1947
end_year_to_filter = 1959

def read_na_jsons(start_at, do_it_for):
    df_product_initial_metadata = pd.DataFrame()
    df_product_extra_metadata = pd.DataFrame()
    df_product_data = pd.DataFrame()

    for i in range(start_at, start_at + do_it_for):
        url = base_url + str(i)
        try:
            initial_product_meta_data = read_meta_data(url)
            start_year = int(initial_product_meta_data[0]["objDate_datingPeriodStartYear_t"])
            
            if((start_year > start_year_to_filter) & (start_year < end_year_to_filter)):
                product_meta_data_file_name = initial_product_meta_data[0]["objDesc_objectName_t"]
                load_data_into_file(initial_product_meta_data, product_meta_data_file_name + ".json")
                df_product_initial_metadata = add_json_to_data_frame(df_product_initial_metadata, initial_product_meta_data)

                df_product_extra_metadata = fetch_and_save_extra_metadata(df_product_extra_metadata, initial_product_meta_data,
                                                                       product_meta_data_file_name)
                product_data = read_product_data(initial_product_meta_data[0]["product_code_t"], product_data_url)
                df_product_data = add_json_to_data_frame(df_product_data, product_data)
                load_attachments(initial_product_meta_data[0], product_meta_data_file_name)
        except:
            continue
    return df_product_data, df_product_initial_metadata, df_product_extra_metadata

def read_product_data(product_code, product_data_url):
  product_data_url = product_data_url.format(product_code)
  return read_meta_data(product_data_url)

def fetch_and_save_extra_metadata(df_product_extra_metadata, initial_product_meta_data, product_meta_data_file_name):
    try:
        sku = initial_product_meta_data[0]["sku"]
        objHier_archiveId_t = initial_product_meta_data[0]["objHier_archiveId_t"]
        extra_product_metadata_url = product_extra_meta_data_url.format(objHier_archiveId_t, sku, sku)
        extra_product_meta_data = read_extra_data_json(extra_product_metadata_url, product_meta_data_file_name + ".json")
        extra_product_meta_data = pd.json_normalize(extra_product_meta_data)
        df_product_extra_metadata = add_json_to_data_frame(df_product_extra_metadata, extra_product_meta_data)
        return df_product_extra_metadata
    except:
        return df_product_extra_metadata

def read_extra_data_json(url, file_name):
    try:
        dir, headers = urllib.request.urlretrieve(url, download_extra_metadata_dir + file_name)
        jfile = open(dir, 'r', encoding='utf8')
        metadata = json.loads(jfile.read())
        jfile.close
        return metadata
    except (Exception):
        raise Exception("Could not read data from the given url")

#generic function to read json data into an object from a give url
def read_meta_data(url):
    try:
        data = urllib.request.urlopen(url)
        if is_valid_data(data):
            metadata = json.loads(data.read())
            docs_data = metadata["response"]["docs"]
            return docs_data
    except:
        raise Exception("Could not open url")

#load the meta data as a json file into external/local location
def load_data_into_file(data_to_load, file_name):
    try:
        with open(download_metadata_dir + file_name, 'w', encoding='utf8') as jfile:
            jfile.write(json.dumps(data_to_load, indent=4, ensure_ascii=False))
            jfile.close()
    except:
        pass

#load files that we've retrieved from the given url into external/local location
def load_attachments(product_data, file_name):
    try:
        file_extension = "." + product_data["objHier_attachment_attachmentType_s"]
        url = base_attachment_url + product_data["attachment_url_s"]
        urllib.request.urlretrieve(url, download_attachment_dir + file_name + file_extension)
    except:
        raise Exception(
            "Could not load the wanted attachment")

def is_valid_data(data):
    return data.getcode() != 404

#add an entry in the data frame for the given data
def add_json_to_data_frame(df, data):
    if df.empty:
        df = pd.DataFrame(data)
    else:
        new_df = pd.DataFrame(data)
        df = df.append(new_df)
    return df

#load files from a local folder into data frame
def read_files_from_local_folder(dir, dataframe, is_extra_metadata):
    file_paths = os.listdir(dir)
    for path in file_paths:
        jfile = open(dir, 'r', encoding='utf8')
        metadata = json.loads(jfile.read())
        jfile.close
        if is_extra_metadata:
          metadata = pd.json_normalize(extra_product_meta_data)
        dataftame = add_json_to_data_frame(dataframe, metadata)
    return dataframe

#Filter out nan values for a given column
def replace_nans(value) :
  return [v if isinstance(v,str) else "אחר" for v in value]

#Handaling miror qriting since our data is in hebrew
def reshape_text(value):
  return [v[::-1] for v in value]

searched_keywords = ["מלחמה", "עלייה", "בן גוריון", "הגירה", "גרמניה", "שואה", "ילדי תימן", "עצמאות", "צנע", "ירושלים", "הסתדרות", "שלום", "פליטים", "מלחמת"]

#Returns list of files which their description contains one of the keywords we look for
def filter_description(object_descriptions, search_keywords):
  return list(filter(lambda description: does_keyword_exits_in_description(description), object_descriptions))

def does_keyword_exits_in_description(description):
  str_description = str(description)
  for keyword in searched_keywords:
    if keyword in str_description:
      return True
  return False

#Create a data frame that counts how many times each keyword appears
def number_of_files_match_keyword(files_description):
  num_of_apperances = []
  df = pd.DataFrame()
  for keyword in searched_keywords:
    num_of_keyword_appearences = 0
    for description in files_description:
      if keyword in str(description):
        num_of_keyword_appearences += 1
    num_of_apperances.append(num_of_keyword_appearences)
        
  return pd.DataFrame({'size': num_of_apperances}, index = reshape_text(searched_keywords))

def prepare_statusea_pie_chart(df):
  temp_df = df
  temp_df.addAttr_statusChasifa_t = reshape_text(replace_nans(temp_df.addAttr_statusChasifa_t))
  df_statuses = temp_df.groupby("addAttr_statusChasifa_t").size()
  df_statuses.plot.pie(y = new_df.addAttr_statusChasifa_t ,shadow = True, fontsize=14,
  figsize=(15,10), autopct='%1.1f%%' )
  plt.title("הפישח סוטטס", fontsize=20)

#Several functions to hadle the persons data withnin our data frame.
#Some items in our data frames contains a list of persons mentioned in the item.
#We extracted all those lists flatten the list of lists and created a data frame that counts how many time each peson is being mentioned.
#After that we created a graph thath displays the top mentioned persons

def filter_nans(values):
  return list(filter(lambda v: pd.isnull(v) == False, values))

def flatten_to_strings(listOfLists):
    result = []
    for i in listOfLists:
        if isinstance(i, six.string_types):
            result.append(i)
        else:
            result.extend(flatten_to_strings(i))
    return result

def get_persons_in_data_frame(person_ss):
    persons_list = []
    for p in person_ss:
      persons_list.append(eval(p))
    return persons_list

def filter_duplicate_persons(list_of_persons):
  p_l_without_dups = []
  for person in list_of_persons:
    if not "(" in person:
      p_l_without_dups.append(person)
  return p_l_without_dups

#Get the index of columns we cany access directly within the data frame.
def column_index(df, query_cols):
    cols = df.columns.values
    sidx = np.argsort(cols)
    return sidx[np.searchsorted(cols,query_cols,sorter=sidx)]

def main():
    df_initial_data, df_extra_meta_data, df_product_data = read_na_jsons(1008796, 200000)

    #join the data frames into one data frame.
    final_merged_df = pd.merge(df_initial_data, df_extra_meta_data, left_on='sku', right_on='objectHierarchy.objectId')

    fig = plt.figure(figsize = (10,10))
    temp_df = number_of_files_match_keyword(final_merged_df.objDesc_objectDesc_t)
    temp_df.plot.barh(fontsize = 12, title = "Top keywords that apeared in document's description")

    fig = plt.figure(figsize = (5,5))
    final_merged_df.objHier_objectType_t.hist()

    #Ceate a graph that displays distrubation of the institutes who created the files
    new_df = final_merged_df
    new_df.objHier_archiveName_t = reshape_text(replace_nans(final_merged_df.objHier_archiveName_t))
    df_origin = final_merged_df.groupby("objHier_archiveName_t").size().nlargest(30)
    df_origin.plot.pie(y = (new_df.objHier_archiveName_t),shadow = True,figsize=(45,40), autopct='%1.1f%%', title="דועיתה רוקמ", fontsize=28, pctdistance=0.85)
    plt.title("דועיתה רוקמ", fontsize=50)

    temp_df = final_merged_df.groupby("objAttr_materialType_t").size()
    temp_df.to_csv("material_type.csv")
    display(temp_df)

    fig = plt.figure(figsize = (20,15))
    temp_df = final_merged_df
    temp_df.addAttr_orgTree_e_t = reshape_text(final_merged_df.addAttr_orgTree_e_t)
    test = temp_df.groupby("addAttr_orgTree_e_t").size()
    temp_df.groupby("addAttr_orgTree_e_t").size().nlargest(20).plot(kind="barh", fontsize=18, title= "Top institutes that documented files" )
    plt.title("Top institutes that documented files", fontsize=30)

    prepare_statusea_pie_chart(final_merged_df)

    fig = plt.figure(figsize = (15,7))
    final_merged_df.groupby("objDate_datingPeriodStartYear_t").size().plot(kind="bar")

    persons_list = get_persons_in_data_frame(filter_nans(final_merged_df.person_ss))
    flatten_persons_list = filter_duplicate_persons(flatten_to_strings(persons_list))
    num_of_ap_per_person = []
    persons_list = []
    for person in flatten_persons_list:
        if not person in persons_list:
            num_of_ap_per_person.append(flatten_persons_list.count(person))
        persons_list.append(person)
    personsDataFrame = pd.DataFrame({"num_of_appearences":num_of_ap_per_person}, index= reshape_text(persons_list))
    personsDataFrame.num_of_appearences.nlargest(20).plot.barh(figsize=(10,10), fontsize= 14)


    matsa_loc = column_index(final_merged_df, ["additionalAttributes.matsa"])
    new_df = final_merged_df
    new_df.iloc[:,matsa_loc[0]] = reshape_text(final_merged_df.iloc[:,matsa_loc[0]])
    new_df.groupby(new_df.iloc[:,matsa_loc[0]]).size().plot.bar(figsize=(5,5), fontsize=15)

    ichsun_loc = column_index(final_merged_df, ["additionalAttributes.ichidatIchsun"])
    new_df = final_merged_df
    new_df.iloc[:,ichsun_loc[0]] = reshape_text(final_merged_df.iloc[:,ichsun_loc[0]])
    new_df.groupby(new_df.iloc[:,ichsun_loc[0]]).size().plot.bar(figsize=(8,5), fontsize=15)

if __name__ == '__main__':

    main()