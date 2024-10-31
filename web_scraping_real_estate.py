import requests
import json
import pandas as pd
from time import sleep
from retrying import retry
from typing import Tuple
from itertools import chain

URL = "https://search-service.fincaraiz.com.co/api/v1/properties/search"
property_type_id = [1,2,14] #1: casa, 2: apartamento, 14: apartaestudio

city_information = [
    {
        "estate_id" : "2d9f0ad9-8b72-4364-a7dc-e161d7dddb4d",
        "estate_name" : "Bogotá, d.c.",
        "estate_slug" : "state-colombia-11-bogota-dc",
        "id" : "65d441f3-a239-4111-bc5b-01c5a268869f",
        "label" : "Bogotá<br/><span style='font-size:12px'>Bogotá, d.c.</span>",
        "coordinates" : [-74.10969158750584,4.656350653340584],
        "city" : "Bogotá",
        "slug" : "city-colombia-11-001"
    },
    {    
        "estate_id" : "baa6a98e-3451-4ae9-b082-3e395a5f0504",
        "estate_name" : "Valle del cauca",
        "estate_slug" : "state-colombia-76-valle-del-cauca",
        "id" : "0e99ce18-9ff5-4c20-9b60-6150cc9e094b",
        "label" : "Cali<br/><span style='font-size:12px'>Valle del cauca</span>",
        "coordinates" : [-76.52160156794767,3.4143822546619274],
        "city" : "Cali",
        "slug" : "city-colombia-76-001"
    }, 
    {    
        "estate_id" : "2d63ee80-421b-488f-992a-0e07a3264c3e",
        "estate_name" : "Antioquia",
        "estate_slug" : "state-colombia-05-antioquia",
        "id" : "183f0a11-9452-4160-9089-1b0e7ed45863",
        "label" : "Medellín<br/><span style='font-size:12px'>Antioquia</span>",
        "coordinates" : [-75.57786065131165, 6.249816589298594],
        "city" : "Medellín",
        "slug" : "city-colombia-05-001"
    }
]


@retry(wait_random_min = 5000, wait_random_max = 10000, stop_max_attempt_number = 3)
def get_data(url: str, request_json: dict)-> Tuple[int, str]:
    """
    Sends a POST request to the given URL with a JSON payload.

    Parameters
    ----------
    url : str
        The URL to send the POST request to.
    request_json : dict
        The JSON payload to include in the POST request.

    Returns
    -------
    tuple of (int, str)
        - status_code: The HTTP status code of the response.
        - response_text: The response text from the server.
    """
    response = requests.post(url,json=request_json)
    status_code = response.status_code
    response_text = response.text

    if status_code == 504:
        raise ValueError('Status code is 504')
    if status_code == 500:
        raise ValueError('No more info')

    return status_code, response_text


def data_extract(city_information : list, property_type_id : list, debug : bool = False) -> list:
    """
    Extracts data based on city information and property types.

    Parameters
    ----------
    city_information : dict
        A dictionary containing information about the city, including its name, ID, estate name, estate ID, estate slug, and label.
    property_type_id : list
        A list of property type IDs to be included in the request.
    debug : bool
        A boolean flag to enable or disable debug mode. If enabled, additional debug information will be printed.

    Returns
    -------
    list
        A list containing the raw data extracted from the requests.
    """
    
    raw_data = []

    for property_type in property_type_id:
        page = 1
        print(f"Property: {property_type}")
        while True:
            #modify json for new request
            request_json = {"variables":{"rows":21,"params":{"page":page,"order":2,"operation_type_id":1,"property_type_id":[property_type],"locations":[{"country":[{"name":"Colombia","id":"858656c1-bbb1-4b0d-b569-f61bbdebc8f0","slug":"country-48-colombia"}],"name":city_information["city"],"location_point":{"coordinates":[city_information["coordinates"][0],city_information["coordinates"][1]],"type":"point"},"id":city_information["id"],"type":"CITY","slug":[city_information["slug"]],"estate":{"name":city_information["estate_name"],"id":city_information["estate_id"],"slug":city_information["estate_slug"]},"label":city_information["label"]}]},"page":page,"source":10},"query":""}
            #new request
            try:
                status_code, response_text = get_data(URL, request_json)
            except ValueError as e:
                print(e, URL, request_json)
                break
            else:
                response_body = json.loads(response_text)
                #add data to empty element
                raw_data.append(response_body['hits']['hits'])
                page += 1
            if debug:
                if page % 10 == 0:
                    print(f"Page: {page}")
        raw_data2 = list(chain(*raw_data))
    return raw_data2


def data_transform(raw_data: dict) -> dict:
    """
    Transforms raw property data by extracting and cleaning relevant fields.

    Parameters
    ----------
    raw_data : dict
        The raw data containing property details as retrieved from the API.

    Returns
    -------
    dict
        A dictionary with the cleaned and relevant property details such as 
        id, price, area, rooms, bathrooms, garage, property type, stratum, 
        location, and city.
    """

    cleaned_element = {}
    cleaned_element['id'] = raw_data['_source']['listing']['id']
    cleaned_element['price'] = raw_data['_source']['listing']['price']['amount']
    cleaned_element['area'] = raw_data['_source']['listing']['m2']
    cleaned_element['rooms'] = raw_data['_source']['listing']['technicalSheet'][7]['value']
    cleaned_element['bathrooms'] = raw_data['_source']['listing']['bathrooms']
    cleaned_element['garage'] = raw_data['_source']['listing']['garage']
    cleaned_element['property_type'] = raw_data['_source']['listing']['property_type']['name']
    cleaned_element['stratum'] = raw_data['_source']['listing']['stratum']
    cleaned_element['location'] = raw_data['_source']['listing']['locations']['location_point']
    cleaned_element['city'] = raw_data['_source']['listing']['locations']['city'][0]['name']

    return cleaned_element


def data_load(df: pd.DataFrame):
    """
    Saves the cleaned DataFrame to a CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the cleaned real estate data.

    Returns
    -------
    None
    """
    df.to_csv('COLOMBIA_REAL_STATE.csv', index = False)

if __name__ == '__main__':
    
    extracted_data = []
    for city in city_information:
        print(f'Extracting {city['city']} info')
        extracted_data.append(data_extract(city, property_type_id))

    extracted_data = list(chain(*extracted_data))
    data_clean = []
    print('Data extracted. Initializing Data cleansing')
    data_cleaned = [data_transform(element) for element in extracted_data]
    data_clean = pd.DataFrame(data_cleaned)
    print('Data cleaned')

    data_load(data_clean)