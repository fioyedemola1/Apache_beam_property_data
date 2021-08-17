# Processing Property Data using Apache_beam

### Expectations
* download the csv data from url link
* present a scalable data processing solution


### Installations

The libray and dependencies used are stored in the requirements.txt. However for full compatibility pls install the full_requirements.txt (has it has the full list of depenceies used)

N.B python 3.9 was used

• Option 1 (using virtualenv)
create virual env:
    python3.9 -m venv [name]
activate:
    windows: [name]\scripts\activate.bat
    mac: source [name]/scripts/activate
run: 
    pip install -r requirements.txt

* Option 2 (using pipenv)
install:
    pip3.9 install pipenv
run:
    pipenv install -r {path to}/requirements.txt 

Option 3:
 Use Pycharm :D

This should install all the necessary dependencies

## Downloading the data

Run the utilty/downloader.py to download part1 & part2  of the csv data.


### processing the pipeline
N.B If previous step has been ommitted you are likely to get a NotImplementedError!!!

The pipeline just simulates a simple read files from csv and does the following steps
1. Files are read using the apache module, based on patterns defined. so therefore, it is able to read multiple files.
2.  converts the list from strings to list: this is to seperate and perform a little data manipulation
3.  performed the intial mapping of the data :
    * creating unique code generated from post_code and more attributes from the propety
    * genreated a full_address
    * assined full detailed names for shortcodes (in hindsight could have been left as is)
    * returned a mutable iterable

4.  Mapped the returned Pcollolections with newly assignd tuples and data types. This comes very handy        especially when sqltransform operations.

5. Pcollections was saved as a text file. nchard could be adjusted.


### TODO

1. Rather than save to inmemory, data could be rerouted and saved to cloud storage or bucket
2. Deduplication: it is important to identify duplicate data:
    * This would give further information on property and raise good questions eg. is profitable how long from the last sale
    * arrive at an average price, is it in decline or increasing in value
                 {'county': 'xxxxxxxxx',
                'currency': 'GBP',
                'date_of_transfer': 'xxxxxxxxx',
                'duration': 'xxxxxxxx',
                'full_address': 'xxxxxxxx  xxxxxxx',
                'new_build': xxxxxx,
                'poan': 'xxxx',
                'post_code': 'xxxxxxxx',
                'price': xxxxxx,
                'avg_price': xxxxx
                'price_paid': 'xxxxxx',
                'property_type': 'xxxxx',
                'record_status': 'xxxxx',
                'uniq_code': 'xxxxxxx'}

3. Grouping data by location to compare prices]
4. generating URPN data, for mapping latitude and longitude
