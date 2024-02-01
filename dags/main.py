from datetime import datetime, timedelta

from airflow import DAG, XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv
from airflow.models import Variable
from docker.types import Mount

import sys
import logging
import os
import requests
import rdflib
import ssl
import json
import pandas as pd
import xmltodict
import xml.etree.ElementTree as ET
from rdflib import URIRef, Literal
from rdflib.namespace import RDF, RDFS
import openpyxl

log = logging.getLogger("airflow.task.operators")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
log.addHandler(handler)

default_args = {
    'owner' : 'red',
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1)
}

def set_key(dictionary, key, value):
     if key not in dictionary:
         dictionary[key] = value
     elif type(dictionary[key]) == list:
         dictionary[key].append(value)
     else:
         dictionary[key] = [dictionary[key], value]

def codelists_from_XML(file_path, output_path):
    with open(file_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    code_lists = data["mes:Structure"]['mes:CodeLists']['str:CodeList']

    output = {}

    for i in range(len(code_lists)):
        list = code_lists[i]
        name_of_list = list['str:Name']['#text']

        codelist_df = {}
        code_data = list['str:Code']
        for x in range(len(code_data)):
            df_result = pd.DataFrame({
                'list_name': name_of_list,
                'urn': code_data[x].get('@urn', 'N/A'),
                'id': code_data[x].get('@id', str(code_data[x].get('@urn', 'N/A').split('.')[-1:][0])),
                'name': code_data[x].get('str:Name', {}).get('#text', 'N/A'),
                'desc': code_data[x].get('str:Description', {}).get('#text', 'N/A')
            }, index=[1])

            codelist_df[x] = df_result
            
        codelist_df_combined = pd.concat(codelist_df.values())
        output[i] = codelist_df_combined

    result = pd.concat(output.values())
    result.columns = ["codelist_name", "urn", "notation", "label", "description"]
    result.to_csv(output_path, encoding='utf-8', index=False)

def xmlToCsvSDMX2_0(input_path, output_path):

    # Looks like I'm gonna have to hard code the metadata at the top for now (TODO)

    with open(input_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    # I hate everything about how I've got the header info here but it works with multiple types of SDMX
        
    header = data["CompactData"]['Header']

    header_dict = {}

    for header, value in header.items():
        if '{' not in str(value):
            header_dict[header.replace('message:', '')] = value
        else:
            for i, j in value.items():
                if '{' not in str(j):
                    header_dict[str(header +  ' ' + i).replace('message:', '').replace('common', '')] = j
                else:
                    for k, l in j.items():
                        if '{' not in str(l):
                            header_dict[str(header +  ' ' + i +  ' ' + k).replace('message:', '').replace('common:', '')] = l
                        else:
                            for m, n in l.items():
                                header_dict[str(header +  ' ' + i +  ' ' + k +  ' ' + m).replace('message:', '').replace('common:', '')] = n

    header_frame = pd.DataFrame([header_dict])

    tables = data["CompactData"]['na_:DataSet']['na_:Series']
    table_header = data["CompactData"]['na_:DataSet']

    headers = []

    for item in table_header.values():
        for i in item[0].items():
            if i[0]!='na_:Obs':
                headers.append(i[0])

    output = []

    for i in range(len(tables)):
        list = tables[i]
        table = list['na_:Obs']
        headers_df = header_frame
        obs_df = pd.DataFrame()

        for i in list.items():
            if i[0] in headers:
                temp = []
                temp.append(i[1])
                headers_df[i[0]] = temp
            else:
                for entry in i[1]:
                    temp_df = pd.DataFrame()
                    for obs in entry.items():
                        temp = []
                        temp.append(obs[1])
                        temp_df[obs[0]] = temp
                    obs_df = pd.concat([obs_df,temp_df])
            
        table_df = pd.concat([headers_df, obs_df], axis = 1)

        output.append(table_df)

    full_table = pd.concat(output)

    headerReplace = [s.replace('@', '') for s in full_table.columns]
    headerNorm = {}
    for key in full_table.columns:
        for value in headerReplace:
            headerNorm[key] = value
            headerReplace.remove(value)
            break
    full_table.rename(columns=headerNorm, inplace=True)

    full_table.to_csv(output_path, encoding='utf-8', index=False)

def xmlToCsvSDMX2_1(input_path, output_path):

    import xmltodict

    with open(input_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    # I hate everything about how I've got the header info here but it works with multiple types of SDMX

    header = data["message:GenericData"]['message:Header']

    header_dict = {}

    for header, value in header.items():
        if '{' not in str(value):
            header_dict[header.replace('message:', '')] = value
        else:
            for i, j in value.items():
                if '{' not in str(j):
                    header_dict[str(header +  ' ' + i).replace('message:', '').replace('common', '')] = j
                else:
                    for k, l in j.items():
                        if '{' not in str(l):
                            header_dict[str(header +  ' ' + i +  ' ' + k).replace('message:', '').replace('common:', '')] = l
                        else:
                            for m, n in l.items():
                                header_dict[str(header +  ' ' + i +  ' ' + k +  ' ' + m).replace('message:', '').replace('common:', '')] = n

    header_frame = pd.DataFrame([header_dict])

    tables = data["message:GenericData"]['message:DataSet']['generic:Series']

    table_frames = []

    for table in tables:
        headers_df = header_frame
        obs_df = pd.DataFrame()    
        for item in table.items():
            table_df = pd.DataFrame()
            if item[0] in ['generic:SeriesKey', 'generic:Attributes']:
                for i in item[1].values():
                    for header in i:
                        values = list(header.values())
                        temp = []
                        temp.append(values[1])
                        headers_df[values[0]] = temp       
            elif item[0] in ['generic:Obs']:
                for i in item[1]:
                    temp_df = pd.DataFrame()
                    for value, key in i.items(): 
                        if str(value) in ['generic:ObsDimension', 'generic:ObsValue']: 
                            temp = []
                            temp.append(list(key.values())[0])
                            temp_df[value] = temp
                        else:
                            for j in list(key.values())[0]:
                                temp = []
                                temp.append(list(j.values())[1])
                                temp_df[list(j.values())[0]] = temp 
                    obs_df = pd.concat([obs_df,temp_df])
                headers_df = pd.concat([headers_df, obs_df], axis = 1)
                table_df = pd.concat([table_df, headers_df])
                table_frames.append(table_df)

    full_table = pd.concat(table_frames, ignore_index=True)
    full_table.to_csv(output_path, index = False)

def wrangling(input_file, output_name):

    if input_file.endswith('.xlsx'):
        df = pd.read_excel(input_file)
    elif input_file.endswith('.csv'):
        df = pd.read_csv(input_file)
    else:
        throw_error("Cannot determine filetype")

    df = df[["EXPENDITURE", "STO", "PRICES", "FREQ", "ADJUSTMENT",
        "REF_AREA", "COUNTERPART_AREA", "REF_SECTOR", "COUNTERPART_SECTOR",
        "ACCOUNTING_ENTRY", "INSTR_ASSET", "ACTIVITY", "UNIT_MEASURE",
        "TRANSFORMATION", "TIME_FORMAT", "REF_YEAR_PRICE", "DECIMALS",
        "TABLE_IDENTIFIER", "UNIT_MULT", "COMPILING_ORG",
        "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "CONF_STATUS"]]

    df.to_csv(output_name +".csv", index=False)

def generate_codelists(codelist_csv, template = None):

    if template is None:
        template = "example-files/src/codelist_template.json"

    df = pd.read_csv(codelist_csv)
    codelist_groupby_df = df.groupby(by="codelist_name")

    for name, group in codelist_groupby_df:
        for row in group.itertuples():
            title = row.codelist_name

        with open(template) as f:
            codelist_template = json.load(f)

        group = group[["label", "notation", "description"]]

        group = group.fillna("")

        codelist = json.loads(group.to_json(orient="records"))

        codelist_template["concepts"] = codelist
        codelist_template["title"] = title

        file_name = title.lower().replace(" ", "_").replace(",", "").replace("(", "").replace(")", "")

        with open(f"example-files/codelists/{file_name}.json", "w") as f:
            json.dump(codelist_template, f, indent=4)

def generate_turtle_from_json_file(json_file_path, output_file):
    # Create an RDF graph
    g = rdflib.Graph()

    # Define namespaces
    skos = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")
    rdfs = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
    rdf = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    owl = rdflib.Namespace("http://www.w3.org/2002/07/owl#")
    cl = rdflib.Namespace("https://example.org/productclassification#")
    xkos = rdflib.Namespace("http://rdf-vocabulary.ddialliance.org/xkos#")

    # Add prefixes to the graph
    g.bind("skos", skos)
    g.bind("rdfs", rdfs)
    g.bind("rdf", rdf)
    g.bind("owl", owl)
    g.bind("cl", cl)
    g.bind("xkos", xkos)

    with open(json_file_path, 'r') as json_file:
        json_data = json.load(json_file)

        # Extract concepts from the "concepts" array
        concepts = json_data.get("concepts", [])
        code_list_title = json_data["title"].lower().replace(" ", "")

        for concept_data in concepts:
            # Create the ConceptScheme for each concept
            concept_scheme = URIRef(f"example.org/{code_list_title}")
            g.add((concept_scheme, rdfs.label, Literal(json_data["title"], lang="en")))
            g.add((concept_scheme, RDF.type, skos.ConceptScheme))
            
            concept = URIRef(f"{cl}{concept_data['notation']}")
            g.add((concept, RDF.type, skos.Concept))
            g.add((concept, skos.prefLabel, Literal(concept_data["label"])))
            g.add((concept, skos.inScheme, concept_scheme))

            if "children" in concept_data:
                # Create Concepts and add them to the graph
                for child in concept_data["children"]:
                    concept = URIRef(f"{cl}{child['notation']}")
                    g.add((concept, RDF.type, skos.Concept))
                    g.add((concept, skos.prefLabel, Literal(child["label"])))
                    g.add((concept, skos.inScheme, concept_scheme))
    
                # Create xkos:hasPart and owl:unionOf relationships
                has_part = cl[concept_data["notation"]]
                for child in concept_data["children"]:
                    g.add((has_part, xkos.hasPart, cl[child["notation"]]))

                """union_of = cl[concept_data["notation"]]
                for child in concept_data["children"]:
                    g.add((union_of, owl.unionOf, cl[child["notation"]]))"""

    # Serialize the graph to the specified output file
    g.serialize("example-files/rdf/" + output_file, format="turtle")

    print(f"RDF data has been serialized to {code_list_title}.ttl")

def generate_editions_metadata(transformedCSV, structureXML, outputPath, config = False):

    # Read in Structure XML and tidyCSV 
    with open(structureXML, 'r') as file:
            xml_content = file.read()
            data = xmltodict.parse(xml_content)

    tidyCSV = pd.read_csv('../example-files/out/SU_SDMX.csv')

    # Pull out the dataset title which we'll use later
    dataset_title = tidyCSV['TITLE'].iloc[0]

    # Get a list of the concepts and key families from the structure XML which contain info on the data dimensions, at the moment we're basically just using this for the datatype and dimension name where possible
    # this then gets thrown into a dictionary with an entry for each possible dimension header
    dimensions = {}

    metadata = data["mes:Structure"]['mes:Concepts']['str:ConceptScheme']['str:Concept']

    for concept in metadata:
        set_key(dimensions, concept['@id'],  concept['@id'].lower())
        set_key(dimensions, concept['@id'],  concept['str:Name']['#text'])
        
    metadata = data["mes:Structure"]['mes:KeyFamilies']['str:KeyFamily']['str:Components']['str:Dimension']

    for concept in metadata:
        set_key(dimensions, concept['@conceptRef'], concept['str:TextFormat']['@textType'])

    # Here we're going through each header in the tidyCSV and attempting to match it up with the DSD informaion we got before, for other headers (such as the metadata columns) we assume the datatype is just a string
    full_dimensions = {}

    for i in tidyCSV.columns:
        if i in dimensions.keys():
            full_dimensions[i] = i
            if len(dimensions[i]) == 3:
                set_key(full_dimensions, i, dimensions[i][0])
                set_key(full_dimensions, i, dimensions[i][1])   
                set_key(full_dimensions, i, dimensions[i][2])          
            else:
                set_key(full_dimensions, i, dimensions[i][0])
                set_key(full_dimensions, i, dimensions[i][1])
                set_key(full_dimensions, i, 'string')
        else:
            full_dimensions[i] = i
            set_key(full_dimensions, i, i.lower())
            set_key(full_dimensions, i, i.lower())
            set_key(full_dimensions, i, 'string')

    full_dimensions_list = list(full_dimensions.values())

    # Read in our metadata template

    with open(r"../example-files/editions_template.json") as json_data:
            editions_template = json.load(json_data)

    # now a very terrible and hardcoded implementation of applying what little metadata we have to as many fields as possible

    editions_template['@id'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions'
    editions_template['title'] = dataset_title

    current_edition = editions_template['editions'][0] # This will get the first entry in the editions list to use as a template (TODO: include editions list length to check if this will be the first edition or an addition and create addendum)
        
    current_edition['@id'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions/' + str(datetime.now().strftime("%Y-%m"))
    current_edition['in_series'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title)
    current_edition['identifier'] = str(datetime.now().strftime("%Y-%m"))
    current_edition['title'] = dataset_title
    current_edition['summary'] = "" # Doesnt appear to have any summary or description in the supporting XML which isnt surprising but means we have nothing for these 2 fields at entry
    current_edition['description'] = ""
    current_edition['publisher'] = "" # not sure whether the sender/reciever covers publisher/creator so will leave this blank for now
    current_edition['creator'] = ""
    current_edition['contact_point'] = {'name': "", 'email' : ""} # Take from config file
    current_edition['topics'] = "" # could we infer this from the structure file?
    current_edition['frequency'] = "" # is this something we should include in the config file?
    current_edition['keywords'] = ["", ""] # anyway some of this could be infered?
    current_edition['issued'] = data["mes:Structure"]['mes:Header']['mes:Prepared'].split('.')[0] + 'Z' # Not sure whether there is a better way to get issued date as it seems to just take the date it was extracted
    current_edition['modified'] = tidyCSV['Extracted'].iloc[0].split('+')[0] + 'Z' # This is working off the assumption that every extraction date is a new modification of the data
    current_edition['spatial_resolution'] = list(tidyCSV.COUNTERPART_AREA.unique()) # this is certainly not gonna be correct in the long run but we can replace it later or remove it 
    current_edition['spatial_coverage'] = list(tidyCSV.REF_AREA.unique()) # this is certainly not gonna be correct in the long run but we can replace it later or remove it 
    current_edition['temporal_resolution'] = list(tidyCSV.TIME_FORMAT.unique())
    current_edition['temporal_coverage'] = {'start' : min(tidyCSV.TIME_PERIOD), 'end' : max(tidyCSV.TIME_PERIOD)} # This will need a lot of formatting/conditions to end up as datetime from what could be varying format of input
    current_edition['versions_url'] = 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions/' + str(datetime.now().strftime("%Y-%m")) + '/versions'
    current_edition['versions'] = {'@id': 'https://staging.idpd.uk/datasets/' + pathify(dataset_title) + '/editions/' + str(datetime.now().strftime("%Y-%m")) + '/versions/1',
                                'issued': data["mes:Structure"]['mes:Header']['mes:Prepared'].split('.')[0] + 'Z'}
    current_edition['next_release'] = "" # could we infer this from issued date and frequency if we include that in the config?

    columns = []
    for i in full_dimensions_list:
        column = {}
        column['name'] = i[0]
        column['datatype'] = i[3]
        column['titles'] = i[1]
        column['description'] = i[2]
        columns.append(column)
    current_edition['table_schema'] = {'columns' : columns}
    editions_template['editions'][0] = current_edition # just a reminder this is currently for a first edition of a dataset so it will put itself as the only entry

    editions_template['count'] = len(editions_template['editions'])
    editions_template['offset'] = 0 # not sure what this is tbh

    # dump out metadata file

    with open(outputPath + pathify(dataset_title) + "_" + str(datetime.now().strftime("%Y-%m")) + ".json", "w") as outfile:
        json.dump(editions_template, outfile, ensure_ascii=False, indent=4)


def throw_error(error):
    raise ValueError(error)


#json_file_path = "/Users/abdulkasim/Documents/data_ingestion_projects/national-accounts/codelists/product_classification.json"
#output_turtle_file = "/Users/abdulkasim/Documents/data_ingestion_projects/national-accounts/rdf/product_classification.ttl"

# Need to change these paths as they wont work right now 

#path_to_json = './example-files/codelists/' 
#json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

#path_to_raw = './example-files/input/' 
#src_files = [pos_json for pos_json in os.listdir(path_to_raw) if pos_json.endswith('.xml')]

with DAG(
    default_args = default_args,
    dag_id = 'CORD-POC_v2',
    description = 'Testing supply use tables',
    start_date = datetime(2024, 2, 1),
    schedule_interval = '@daily'
) as dag:
    #if len(src_files) == 1:

        #src_file = src_files[0]

        codelistsFromXML = PythonOperator(
            task_id = 'codelistsFromXML',
            python_callable = codelists_from_XML,
            op_args=['./example-files/XMLs/NA_SU_v1.9_SDMX2.0 OT.xml', "example-files/out/CodeLists.csv"]
        )
        
        XMLtoCSV = PythonOperator(
            task_id = 'convertXMLtoCSV',
            python_callable = xmlToCsvSDMX2_0,
            op_args=[r'./example-files/input/SUT T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_25_Jan_2024 (SDMX 2.0).xml', "example-files/out/SU_SDMX.csv"]
        )

        editionsMetadata = PythonOperator(
            task_id = 'generateEditionsMetadata',
            python_callable = generate_editions_metadata,
            op_args = ("./example-files/out/SU_SDMX.csv", r"./example-files/input/NA_SU_v1.9_SDMX2.0 OT.xml", "example-files/out/")
        )

        generateCodelists = PythonOperator(
            task_id = 'generateCodelists',
            python_callable = generate_codelists,
            op_args=["example-files/out/CodeLists.csv"]
        )
        
        codelistsFromXML >> XMLtoCSV >> editionsMetadata >> generateCodelists

        #CSVWrangling = PythonOperator(
        #        task_id = 'CSVWrangling',
        #        python_callable = wrangling,
        #        op_args=["example-files/out/SU_SDMX.csv", 'example-files/out/SU_SDMX_tidy.csv']
        #    )
         
        #codelistsFromXML >> XMLtoCSV #>> CSVWrangling
            
        

        # where is the below .json file coming from? is it just handmade? in which case are we gonna have to handmake these for every file?
        # 
        #csvcubed = BashOperator(
        #        task_id = 'csvcubed',
        #        bash_command = 'csvcubed build ${AIRFLOW_HOME}/example-files/na_2019Q3.csv -c na_2019Q3.json --validation-errors-to-file',
        #        cwd='example-files'
        #    )  

        #xmlToCsv# >> excelWrangling >> generateCodelists# >> csvcubed

        #for file in json_files:

        #    generateTurtleFromJsonFile = PythonOperator(
        #        task_id = 'generateTurtleFromJsonFile_{}'.format(file),
        #        python_callable = generate_turtle_from_json_file,
        #        op_args=['example-files/codelists/' + file, file.split('.')[0] + '.ttl']
        #    )

        #    generateCodelists >> generateTurtleFromJsonFile

    #else:
     #       multipleFilesError = PythonOperator(
      #          task_id = 'Error',
       #         python_callable = throw_error,
        #        op_args=["Multiple/No Input files detected, please ensure only 1 source file is present."]                
         #   )  