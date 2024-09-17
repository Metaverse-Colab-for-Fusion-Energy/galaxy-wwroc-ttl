import json
import jq
import zipfile
import requests
import rdflib
import os
import arcp
import uuid

def upload_crate_provenance(rocrate_filepath, temp_unzip_dir, user):
    '''
    Unzip the crate, combine provenance to the crate and then upload to triple store.

    Data added:
    - dataset_attrs.txt
    - username

    Parameters:
    filepath (str): The path to the crate.
    temp_unzip_dir (str): The path to the directory where the crate will be unzipped.
    nucleus_crate_url (str): The base URL of the crate's location on nucleus.
    user (dict): Dict containing user info for the user who ran the workflow.
    '''

    # --------------------------------------------------------------
    # Generate a new UUID for the crate

    crate_uuid = uuid.uuid4()

    # --------------------------------------------------------------
    # Unzip the RO-Crate
    with zipfile.ZipFile(rocrate_filepath, "r") as zip_ref:
        zip_ref.extractall(temp_unzip_dir)

    crate_path = temp_unzip_dir

    # --------------------------------------------------------------
    # Combine the provenance data from the WRROC to the RO-Crate

    # Load the crate metadata as json
    with open(f'{crate_path}/ro-crate-metadata.json') as f:
        data = json.load(f)

    # Load the provenance files as json
    with open(f'{crate_path}/datasets_attrs.txt') as f:
        dataset_attrs = json.load(f)

    # Form the jq filters
    nucleus_crate_url = f"https://{crate_uuid}/"
    filter_dataset_attrs = f'.[] | {{"@id": .file_name, "@type": "File", dateCreated: .update_time, url: ("{nucleus_crate_url}" + .file_name), exampleOfWork: {{"@id": ("#" + .dataset_uuid)}}, name: .name}}'
    filter_dataset_attrs_example = '.[] | {"@id": ("#" + .dataset_uuid), "@type": "FormalParameter", "additionalType": "File", "description": "", name: .name}'

    # Apply the jq filter to the provenance data
    dataset_attrs_file = jq.compile(filter_dataset_attrs).input(dataset_attrs).all()
    dataset_attrs_formal_param = jq.compile(filter_dataset_attrs_example).input(dataset_attrs).all()

    # Add the output provenance to the ro-crate-metadata.json
    # converting the list of dictionaries to a json object
    # and append information to input datasets
    output_ids = []
    output_files = []
    ids = []
    for jj in data['@graph']:
        ids.append(jj['@id'])
        # If id ends with .gxwf.yml then it is the workflow id
        if jj['@id'].endswith('.gxwf.yml'):
            workflow_id = jj['@id']

    for ii in dataset_attrs_file:
        if ii['@id'] not in ids:
            data['@graph'].append(ii)
            output_files.append(ii['@id'])
            for file in dataset_attrs_formal_param:
                if file['@id'] == ii['exampleOfWork']['@id']:
                    data['@graph'].append(file)
                    output_ids.append(file['@id'])
        else:
            for jj in data['@graph']:
                if jj['@id'] == ii['@id']:
                    jj['dateCreated'] = ii['dateCreated']
                    jj['url'] = ii['url']

    creator = {'@id': user['username']}

    # Add outputs to the workflow
    # and Add the person to the ro-crate-metadata.json in the workflow
    for jj in data['@graph']:
        if jj['@id'] == workflow_id:
            jj['output'] = []
            for id in output_ids:
                jj['output'].append({"@id": id})
            jj['creator'] = creator
        if jj['@id'] == './':
            for file in output_files:
                jj['hasPart'].append({"@id": file})

    # Write the updated ro-crate-metadata.json
    with open(f'{crate_path}/ro-crate-metadata-updated.json', 'w') as f:
        json.dump(data, f, indent=4)

    # Load the updated ro-crate-metadata.json using rdflib
    g = rdflib.Graph()
    g.parse(f'{crate_path}/ro-crate-metadata-updated.json',
            format='json-ld',
            publicID=f'{arcp.generate.arcp_uuid(crate_uuid)}'
            )

    return g


if __name__ == '__main__':
    tmp_dir = '/tmp/temp_unzip_dir'

    graph = rdflib.Graph()

    # for file in path ./crates
    for crate in os.listdir('./crates/'):
        crate_path = f'./crates/{crate}'
        new_graph = upload_crate_provenance(crate_path, tmp_dir, {'username': 'user2341'})
        graph += new_graph

    # Serialize the graph to a string
    rdf_string = graph.serialize(format='turtle')

    # Serialise to file
    graph.serialize(f'./wrroc.ttl', format='turtle')
    

