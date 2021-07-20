"""Contains helper functions for getting data from Illumina BaseSpace."""
import requests
import logging
import os
import json
import tempfile
import ast
from Aries.storage import StorageFile
from Aries.strings import Base64String
from Aries.web import WebAPI

logger = logging.getLogger(__name__)

API_SERVER = "https://api.basespace.illumina.com/"
bs_api = None


def get_access_token():
    """Gets the BaseSpace access token
    """
    credentials = os.environ.get("BASESPACE_CREDENTIALS")
    if not credentials:
        raise EnvironmentError(
            "BASESPACE_CREDENTIALS must be set in system environment variables."
        )
    # Credential is base64 encoded in environ if it starts with "ew"
    if credentials.startswith("ew"):
        credential_dict = ast.literal_eval(Base64String(credentials).decode_to_string())
    else:
        if os.path.exists(credentials):
            with open(credentials, "r") as credentials_json:
                credential_dict = json.load(credentials_json)
        else:
            return credentials
    return credential_dict.get("access_token")


def build_api_url(relative_url, **kwargs):
    """Builds the URL for BaseSpace API.

    Args:
        relative_url (str): The relative url of BaseSpace API endpoint, e.g. "v1pre3/files/1863963".
            In the BaseSpace API response, the API for additional data is usually in the "Href" field.
        **kwargs: Additional parameters for the API. They will be encoded in the GET request URL.

    Returns: The full URL for making API request.

    """
    access_token = get_access_token()
    bs_api = WebAPI(API_SERVER, access_token=access_token)
    
    url = bs_api.build_url(relative_url, **kwargs)
    return url


def get_response(url):
    """Makes HTTP GET request and gets the response content of the BaseSpace API response.
    This function makes the HTTP GET request.
    If the request is successful, the BaseSpace API response is in JSON format.
    Each JSON response contains a "Response" key, for which the value is the actual response content of the API.
    This function returns the value of the "Response" key.

    Args:
        url (str): The HTTP GET request URL.

    Returns: A dictionary containing the the value of the "Response" key in the JSON response.

    """
    logger.debug("BaseSpace API: %s" % url)
    r = requests.get(url)
    if r.status_code == 200:
        return r.json().get("Response")
    else:
        return None


def get_integer(dictionary, key):
    """Gets value of a key in the dictionary as a integer.

    Args:
        dictionary (dict): A dictionary.
        key (str): A key in the dictionary.

    Returns: A integer, if the value can be converted to integer. Otherwise None.

    """
    val = dictionary.get(key)
    try:
        return int(val)
    except ValueError:
        return None


def api_response(href, *args, **kwargs):
    """Makes HTTP GET request and gets the response content of the BaseSpace API response.
    This function accepts the API address as input, e.g. "v1pre3/files/1863963".
    If *args are specified, this function will continue to make requests using the api returned in the response.
    For example, if *args = ("Href", "HrefContent"), this function will:
        1. Gets the first response using the API in the href argument.
        2. Gets the "Href" field from the first response and make another request using its value.
        3. Gets the "HrefContent" field from the second response and make another request using its value.


    Args:
        href (str): The BaseSpace API, e.g. "v1pre3/files/1863963".
        *args: Keys for getting APIs for subsequent requests.
        **kwargs: The same parameters will be applied to all api calls.

    Returns: The final BaseSpace API response.

    """
    url = build_api_url(href, **kwargs)
    response = get_response(url)
    for arg in args:
        if response:
            href = response.get(arg)
            if href:
                url = build_api_url(href)
                response = get_response(url)
    return response


def api_collection(href):
    """Makes requests to BaseSpace API and gets all items in a collection.
    The BaseSpace API limits the number of items returned in each request.
    This function makes multiple requests and gets all the items.
    Use this function with caution when there are many items in a collection.

    Args:
        href (str): The BaseSpace API for a collection of items, e.g. "/v1pre3/projects/12345/samples".

    Returns: A list of items (dictionaries) in the collection.

    """
    items = []
    batch_limit = 1024
    total_count = 1
    displayed_count = 0
    offset = 0

    while total_count is not None and displayed_count is not None and offset < total_count:
        url = build_api_url(href, Limit=batch_limit, Offset=offset)
        response = get_response(url)
        if response is None:
            # There is likely an error when response is None.
            return None
        batch = response.get("Items", [])
        if batch:
            items.extend(batch)
        total_count = get_integer(response, "TotalCount")
        displayed_count = get_integer(response, "DisplayedCount")
        offset = offset + displayed_count
    return items


def transfer_file(to_folder_uri, file_id=None, file_info_href=None):
    # Determine the file_id, file_info and file_content_href
    if file_id is not None:
        file_info_href = "v1pre3/files/%s" % file_id
        file_content_href = "v1pre3/files/%s/content" % file_id
    elif file_info_href is not None:
        file_id = file_info_href.strip("/").split("/")[-1]
        file_content_href = "%s/content" % file_info_href
    else:
        raise ValueError("Either BaseSpace file_id or file_info_href is needed for file transfer.")

    file_info = api_response(file_info_href)
    logger.debug("Transferring file from BaseSpace: %s" % file_content_href)

    # For FASTQ files, add basespace file ID to filename
    # Each MiSeq run may have multiple FASTQ files with the same name.
    filename = file_info.get("Name")
    if filename.endswith(".fastq.gz"):
        filename = filename.replace(".fastq.gz", "_%s.fastq.gz" % file_id)

    # Skip if a file exists and have the same size.
    to_uri = os.path.join(to_folder_uri, filename)
    dest_file = StorageFile(to_uri)
    file_size = file_info.get("Size")
    if file_size and dest_file.exists() and dest_file.size and dest_file.size == file_info.get("Size"):
        logger.debug("File %s exists at destination: %s" % (filename, to_uri))
        return to_uri
    from_uri = build_api_url(file_content_href)
    StorageFile(from_uri).copy(to_uri)
    return to_uri
