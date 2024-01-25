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



def codelists_from_XML(file_path, output_path):
    with open(file_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    code_lists = data["mes:Structure"]['mes:CodeLists']['str:CodeList']

    output = {}

    for i in range(len(code_lists)):
        list = code_lists[i]
        name_of_list = list['str:Name']['#text']
        print(name_of_list)

        codelist_df = {}
        code_data = list['str:Code']
        for x in range(len(code_data)):
            df_result = pd.DataFrame({
                'list_name': name_of_list,
                'urn': code_data[x].get('@urn', 'N/A'),
                'id': code_data[x].get('@id', 'N/A'),
                'name': code_data[x].get('str:Name', {}).get('#text', 'N/A'),
                'desc': code_data[x].get('str:Description', {}).get('#text', 'N/A')
            }, index=[1])

            codelist_df[x] = df_result
        
        codelist_df_combined = pd.concat(codelist_df.values())
        output[i] = codelist_df_combined

    result = pd.concat(output.values())
    result.columns = ["codelist_name", "urn", "notation", "label", "description"]
    result.to_csv(output_path, encoding='utf-8', index=False)

def xmlToCsvSTANDARDSDMX(input_path, output_path):

    # Looks like I'm gonna have to hard code the metadata at the top for now (TODO)

    with open(input_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    # I hate everything about how I've got the header info here but it works with multiple types of SDMX
        
    header = data["CompactData"]['Header']

    print(header)

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

def xmlToCsvCORDSDMX(input_path, output_path):

    import xmltodict

    with open(input_path, 'r') as file:
        xml_content = file.read()
        data = xmltodict.parse(xml_content)

    # I hate everything about how I've got the header info here but it works with multiple types of SDMX

    header = data["message:GenericData"]['message:Header']

    print(header)

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



def throw_error(error):
    raise ValueError(error)


#json_file_path = "/Users/abdulkasim/Documents/data_ingestion_projects/national-accounts/codelists/product_classification.json"
#output_turtle_file = "/Users/abdulkasim/Documents/data_ingestion_projects/national-accounts/rdf/product_classification.ttl"

# Need to change these paths as they wont work right now 

path_to_json = './example-files/codelists/' 
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

path_to_raw = './example-files/src/' 
src_files = [pos_json for pos_json in os.listdir(path_to_raw) if pos_json.endswith('.csv')]

with DAG(
    default_args = default_args,
    dag_id = 'CORD-POC_v1',
    description = 'Testing supply use tables',
    start_date = datetime(2024, 1, 22),
    schedule_interval = '@daily'
) as dag:
    if len(src_files) == 1:

        src_file = src_files[0]

        codelistsFromXML = PythonOperator(
                task_id = 'codelistsFromXML',
                python_callable = codelists_from_XML,
                op_args=[r"example-files/XMLs/T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_24_Jan_2024.xml", "example-files/out/CodeLists.csv"]
            )
        
        XMLtoCSV = PythonOperator(
            task_id = 'convertXMLtoCSV',
            python_callable = xmlToCsvCORDSDMX,
            op_args=[r"example-files/XMLs/T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_24_Jan_2024.xml", "example-files/out/SU_SDMX.csv"]
        )

        #CSVWrangling = PythonOperator(
        #        task_id = 'CSVWrangling',
        #        python_callable = wrangling,
        #        op_args=["example-files/out/SU_SDMX.csv", 'example-files/out/SU_SDMX_tidy.csv']
        #    )
         
        codelistsFromXML >> XMLtoCSV #>> CSVWrangling
            
        #generateCodelists = PythonOperator(
        #        task_id = 'generateCodelists',
        #        python_callable = generate_codelists,
        #        op_args=['example-files/CodeLists.csv']
        #    )
        
        #codelistsFromXML >> CSVWrangling >> generateCodelists

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

    else:
            multipleFilesError = PythonOperator(
                task_id = 'Error',
                python_callable = throw_error,
                op_args=["Multiple Input files detected, please ensure only 1 source file is present."]                
            )  