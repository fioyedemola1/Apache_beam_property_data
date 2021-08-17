import requests
import csv


def request_url(num: int)-> str:
    if isinstance(num, int):
        url: str = f'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2021-part{num}.csv'
        return requests.get(url)
    

def download_csv():
    for i in range(1,3):
        response = request_url(i)
        if response.status_code==200:
            with open(f'./pp-2021-part{i}.csv','wb') as file:
                file.write(response.content)
              
                    

if __name__ == '__main__':
    download_csv()