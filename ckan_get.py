import requests
import csv
import pandas as pd


def get_organizations():
    r = requests.get("http://catalogo.datos.gob.mx/api/3/action/organization_list")
    organizaciones = r.json()["result"]
    return organizaciones


def get_ckan_data_organization(organization):
    url_string = "http://catalogo.datos.gob.mx/api/3/action/organization_show?id=" + organization
    w = requests.get(url_string)
    return w.json()["result"]


def get_key(dictionary,key,subkey = None):
    if key in dictionary:
        if subkey is not None:
            data = dictionary[key][subkey]
        else:
            data = dictionary[key]
    else:
        data = "N/A"
    return data


def get_key_from_extras(dictionary,key):
    try:
        extras = dictionary["extras"]
        datos = {}
        for value in extras:
            datos[value["key"]] = value["value"]
        if key in datos:
            data = datos[key]
        else:
            data = "N/A"
    except:
        data = "N/A"
    return data


def get_tags(dictionary):
    try:
        tags = dictionary["tags"]
        datos = ""
        for tag in tags:
            datos = datos + tag["display_name"] +" ,"
        if datos!= "":
            data = datos
        else:
            data = "N/A"
    except:
        data = "N/A"
    return data


def tracking_summary_data(dictionary,value):
    track = dictionary["tracking_summary"]
    datos = get_key(track,value)
    return datos


def get_resource_list(dictionary):
    datos = get_key(dictionary,"resources")
    return datos


def get_dataset_data(dictionary):
    full_info = []
    dataset_info = []
    dataset_keys = ["name","title","notes","num_resources"]
    organization_keys = ["name","description","title","type"]
    extras_keys = ["dcat_modified","dcat_publisher_email","dcat_publisher_name","guid","language"]
    resource_keys = ["name","description","size","format","id","url"]
    for key in dataset_keys: dataset_info.append(get_key(dictionary,key))
    for key in organization_keys: dataset_info.append(get_key(dictionary,"organization",key))
    for key in extras_keys: dataset_info.append(get_key_from_extras(dictionary,key))
    dataset_info.append(get_tags(dictionary))
    for resource in dictionary['resources']:
        resource_info = []
        track = []
        for key in resource_keys: resource_info.append(get_key(resource,key))
        csv_info = get_headers_and_type(resource_info[-3],resource_info[-2],resource_info[-1])
        track.append(tracking_summary_data(resource,"recent"))
        track.append(tracking_summary_data(resource,"total"))
        full_info.append(dataset_info + resource_info + csv_info + track)
    return full_info


def DownloadFile(url,local_filename):
    try:
        r = requests.get(url)
        failed = 0
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
    except:
        failed = 1
    return failed


def get_headers_and_type(file_format,resource_id,file_url):
    datastore = call_datastore(resource_id)
    if datastore["success"] == "true":
        api = 1
        fields_value = get_datastore_values(datastore["result"])
    else:
        api = 0
        if file_format == "CSV":
            fields_value = get_csv_values(file_url)
        else:
            fields_value = "NA"
    return [api,fields_value]


def call_datastore(resource_id):
    r = requests.get("http://catalogo.datos.gob.mx/api/action/datastore_search?resource_id=" + resource_id)
    datastore = r.json()
    return datastore


def get_datastore_values(dictionary):
    fields = dictionary["fields"]
    fields_string = ""
    for field in fields:
        fields_string= fields_string + field["id"] +","+ field["type"] + ";"
    return fields_string


def get_csv_values(url):
    failed_download = DownloadFile(url,"local.csv")
    fields_string = ""
    if failed_download == 0:
        csv_file = None
        try:
            csv_file = pd.DataFrame.from_csv("local.csv")
        except:
            try:
                csv_file = pd.DataFrame.from_csv("local.csv",encoding="latin_1")
            except:
                try:
                    csv_file = pd.DataFrame.from_csv("local.csv",encoding="ascii")
                except:
                    fields_string = "N/A"
        if fields_string != "" and csv_file is not None:
            csv_columns = csv_file.columns.values
            i = 0
            for name in csv_columns:
                fields_string= fields_string + name +","+ str(csv_file.dtypes[i]) + ";"
                i = i +1
    else:
        fields_string = "N/A"
    return fields_string

def print_csv_output(table):
    df = pd.DataFrame(table)
    df = df.transpose()
    df.to_csv("output_data.csv",index=False)
    return None

def main():
    organizations = get_organizations()
    dataset_data = []
    for organization in organizations:
        print(organization)
        org_dict = get_ckan_data_organization(organization)
        for datasets in org_dict["packages"]:
            for resources in get_dataset_data(datasets):
                dataset_data.append(resources)
    print_csv_output(dataset_data)
    print("Finished")
    return None

if __name__ == "__main__":
    main()
