# Real Estate Data Extraction and Transformation

This Python script extracts, transforms, and saves real estate data from the FincaRaiz API for selected cities in Colombia. It targets property listings for Bogotá, Cali, and Medellín and includes information such as price, area, rooms, and other essential attributes.

## Features

- **Data Extraction**: Uses POST requests to pull real estate data based on city and property type from the FincaRaiz API.
- **Data Transformation**: Processes raw data to extract essential details (price, area, rooms, bathrooms, location, etc.).
- **Data Loading**: Saves the cleaned and structured data as a CSV file (`COLOMBIA_REAL_STATE.csv`) for further analysis.

## Prerequisites

- Python 3.6+
- Required Python libraries:
  - `requests`
  - `pandas`
  - `retrying`

You can install these dependencies with:
```bash
pip install requests pandas retrying
```

## Script Overview

### Structure

The script consists of three main functions:

1. **`data_extract(city_information: list, property_type_id: list, debug: bool = False) -> list`**: Extracts data based on the provided city and property type parameters. The extracted data is returned as a list.
  
2. **`data_transform(raw_data: dict) -> dict`**: Processes raw data to extract and clean fields such as property ID, price, area, rooms, bathrooms, property type, etc.
  
3. **`data_load(df: pd.DataFrame)`**: Saves the final cleaned data into a CSV file.

### Usage

The script iterates through each city and property type, extracts the necessary data, and processes it as follows:
```python
if __name__ == '__main__':
    extracted_data = []
    for city in city_information:
        print(f'Extracting {city["city"]} info')
        extracted_data.append(data_extract(city, property_type_id))
    
    extracted_data = list(chain(*extracted_data))
    print('Data extracted. Initializing data cleansing')
    data_cleaned = [data_transform(element) for element in extracted_data]
    data_clean = pd.DataFrame(data_cleaned)
    print('Data cleaned')
    data_load(data_clean)
```

### Parameters

- **City Information**: A list of dictionaries containing city data (ID, name, coordinates).
- **Property Type ID**: List of property types to filter (1: house, 2: apartment, 14: studio apartment).
- **Debug Mode**: Enables logging for additional visibility into the process.

## Error Handling

- The script uses the `retrying` library to handle retry attempts with exponential backoff for failed requests, up to 3 retries.

## Output

- **COLOMBIA_REAL_STATE.csv**: Contains structured data of real estate listings with fields like price, area, rooms, location, etc.

## Example Usage

```bash
python real_estate_extraction.py
```

This command will initiate the data extraction, transformation, and loading process.

## License

This project is licensed under the MIT License.