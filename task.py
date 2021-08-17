

import os
import re
import logging
import argparse
import apache_beam as beam
from typing import NamedTuple,Optional,TypeVar,Dict,Iterable,List





M= TypeVar('M')





class ExtractText(beam.DoFn):
    def process(self,element):
        row = element.split(',')
        pattern= re.compile((r'\{+|\}+|\"+'))
        new_row = [re.sub(pattern,'',item) for item in row ]
        yield new_row








def data_mapping(element:beam.PCollection[str]):
    """
        Transforms Pcollection data read, desstructure and maps.

                {'county': 'GREATER LONDON',
                'currency': 'GBP',
                'date_of_transfer': '2021-02-26 00:00',
                'duration': 'FREEHOLD',
                'full_address': '85 CRANHAM ROAD  HORNCHURCH HAVERING',
                'new_build': False,
                'poan': '85',
                'post_code': 'RM11 2AD',
                'price': 228577,
                'price_paid': 'STANDARD PRICE',
                'property_type': 'TERRACED',
                'record_status': 'ADDITION',
                'uniq_code': 'RM112ADTNF85'}
    NB: the transaction details field is dropped as it has no inherent value to our data. Except creating 
        unique identity for each property
    """

    _,price,date_of_transfer,post_code,property_type,new_property,duration,poan,soan,*address,county,ppd,record_status = element
    price = int(price) if price else 0
    uniq_code= f"{''.join(post_code.replace(' ',''))}{property_type}{new_property}{duration}{poan}"
    property_type = property_name.get(property_type,'')
    duration = duration_name.get(duration,'')
    new_property = age_prop.get(new_property,'')

    if poan:
        full_address = [poan,soan, *address] 
    address1 = [poan,*address]
    address1 = ' '.join(address1)
    ppd = category.get(ppd,'')
    record_status = rec_status.get(record_status,'')
    mapped= [uniq_code,date_of_transfer,post_code,property_type,price,new_property,duration,poan,county,ppd,record_status,address1]

    yield mapped






class Property(NamedTuple):
    uniq_code: str
    date_of_transfer : str
    post_code : str
    property_type : str
    price : int
    new_build : bool
    duration : str
    poan : str
    county: str
    price_paid : str
    record_status: str
    full_address : str
    currency: Optional[str]= 'GBP'




duration_name ={
    'F':'FREEHOLD',
    'L':'LEASEHOLD',
}

property_name= {
    'D':'DETACHED',
    'S':'SEMI-DETACHED',
    'T':'TERRACED',
    'F':'FLATS/MAISONETTES',
    'O':'OTHER'
}

age_prop ={
    'Y':True,
    'N':False,
}
category ={
    'A': 'STANDARD PRICE',
    'B': 'ADDITIONAL PRICE',
}
rec_status ={
    'A': 'ADDITION',
    'B': 'CHANGE',
    'C': 'DELETE',
}





def run(directory: str,output_file: Optional[str]='output'):

    """ 
    building Pipeline 

    It takes in a pattern of filepath. It can read multiple files having same file pattern.
    
    The output to csv was left as a dictionary. Expectation is due to the volume of data handled, 
    it could be better passed through Bigquery or cloud storage rather than local memory.
    
    """
    FILE_PATTERN = f'{directory}/*.csv'
    print(FILE_PATTERN)

    with beam.Pipeline() as pipeline:
        readings = (pipeline | beam.io.textio.ReadFromTextWithFilename(file_pattern=FILE_PATTERN,compression_type='auto' )
                            | beam.Map(lambda x: x[1]))
                            

        mapping = (readings
                            
                            |'csv_data extraction' >>beam.ParDo(ExtractText())
                            |'mapping extracted data' >> beam.ParDo(data_mapping)
                            |'transforming to dict' >> beam.Map(lambda x: Property(*x)._asdict() ).with_output_types(Property)
                            # |beam.Map(pprint)
                            |beam.io.WriteToText(
                            file_path_prefix=output_file,
                            
                            )
        )




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)     
    # output_file= os.path.join(directory,'new_data') # Not implemented in python 3.9---> output_file = beam.io.filesystem.FileSystem.join(directory,'new_data') 
    directory = os.getcwd()
    run(directory)  
        
        
        
        
        
        
        
        
        