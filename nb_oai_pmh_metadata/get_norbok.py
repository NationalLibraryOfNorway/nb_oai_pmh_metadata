"""Script for scraping norbok oai-pmh feed.
"""

from bs4 import BeautifulSoup
import requests

START_URL = "https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NETWORK/request?verb=ListRecords&metadataPrefix=marc21&set=norbok"
RESUMPTION_URL = "http://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NETWORK/request?verb=ListRecords&resumptionToken={resumption_token}"




def main():
    
    url = START_URL
    
    
    count = 1 # Counter for the files
    
    while True:
        
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "lxml-xml")
     
        # Save the response to a file
        with open(f"norbok/norbok_{count}.xml", "w") as f:
            f.write(res.text)
        count += 1
        
        
        # Check if there is a resumption token
        try:
            resumption_token = soup.find("resumptionToken").text
        except:
            resumption_token = False
        
        
        # If there is a resumption token, update the url
        if resumption_token:
            url = RESUMPTION_URL.format(resumption_token=resumption_token)
        else:
            print("No resumption token found. Exiting.")
            break    
        
if __name__ == "__main__":
    main()
