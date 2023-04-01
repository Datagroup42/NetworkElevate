import requests
from bs4 import BeautifulSoup
import wikipedia
import spacy
import wikipediaapi
import urllib.parse
import re

# Initialize Wikipedia API
wiki = wikipediaapi.Wikipedia('en')

# Get the text of a Person page
page_title = input("Enter a Person Name: ")

host_url = "https://en.wikipedia.org/wiki/"

# fetch the page object
page = wiki.page(page_title)

# fetch the content of the page
page_text = page.text


# Load the pre-trained NER model
nlp = spacy.load("en_core_web_sm")

# Process the text using the NER model
doc = nlp(page_text)

# Define a set to store the distinct person names
person_names = set()

# Iterate over the entities recognized by the NER model
for ent in doc.ents:
    # Check if the entity is a person and is not the name of the person from which we are taking data
    if ent.label_ == "PERSON" and ent.text != page_title:
        # Add the person name to the set
        person_names.add(ent.text)


#person_names


main_node = page_title

# Dictionary to store name-link pairs
name_link_dict = {}

# Loop over all names
for name in person_names:
    if len(name.split()) > 1:
      # Check if page exists for the given name
      if wiki.page(name).exists():
          # Get the page URL and title
          page_url = wiki.page(name).fullurl
          page_title = wiki.page(name).title
          
          # Check if the URL is different from the page we are currently extracting
          if urllib.parse.unquote(page_url) != host_url + urllib.parse.quote(page_title):
              name_link_dict[page_title] = page_url
#       else:
#           print(f"No Wikipedia page found for {name}")
          
# Print the resulting name-link dictionary
#name_link_dict


# Print names without links
no_url_people = {x for x in person_names if x not in name_link_dict}
no_url_people

list_name = []
def get_infobox_details(key, value):
    # Send a GET request to the URL and store the response
    #print(key)
    response = requests.get(value)

    # Parse the HTML content of the response using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the infobox vcard element using its class name
    infobox = soup.find('table', {'class': 'infobox biography vcard'})
    if infobox is None:
      infobox = soup.find('table', {'class': 'infobox vcard'})
    if infobox is None:
      return {}

    
    # Create an empty dictionary to store the details
    details = {}

    details['Name'] = key
    list_name.append(key)
    # Loop through all the rows in the infobox table
    for row in infobox.find_all('tr'):
        # Get the th and td elements from the row
        th = row.find('th')
        td = row.find('td')

        # Check if both th and td elements are present
        if th is not None and td is not None:
            # Extract the text content of th and td elements
            key = th.get_text().strip()
            value = td.get_text().strip()

            # Store the key-value pair in the dictionary
            details[key] = value


    return details


result_list = []
# URL of the Wikipedia page to fetch the infobox from
url_title = re.sub(r"\s+", "_", main_node) 
url = host_url + url_title
info = get_infobox_details(main_node, url)
result_list.append(info)

result_list

for key, value in name_link_dict.items():
    result = get_infobox_details(key, value)
    result_list.append(result)


result_list

# keep only 'name' and 'city' key-value pairs
filtered_data = [{k: v for k, v in d.items() if k in ['Name', 'Born', 'Education', 'Occupations', 'Spouse', 'Children', 'Parents']} for d in result_list]


#filtered_data

import pandas as pd
df = pd.DataFrame(filtered_data)
df = df.dropna(axis=0, how='all')

# df

df.to_csv("Node_Prop.csv", encoding='utf-8', index=False)

import pandas as pd
import csv
from neo4j import GraphDatabase
driver=GraphDatabase.driver(uri="bolt://localhost:7687",auth=("neo4j","Demo@123"))
session=driver.session()

df=pd.read_csv("Node_Prop.csv")

q0 = "MATCH (n) DETACH DELETE n"
result = session.run(q0)

q1="MERGE (:PERSON{NAME:$Name,JOB_TITLE:$Job_Title,BIRTH_DATE:$Birth_Date,BIRTH_PLACE:$Birth_Place})"
q2='''MATCH (a:PERSON{NAME:$Name})
MATCH (b:PERSON{NAME:$Related_to})
MERGE (a)-[:RELATIONSHIP]->(b)
RETURN a,b
'''

with open("Node_Prop.csv")as f1:
    data=csv.reader(f1,delimiter=",")
    v=[]
    i=0
    for row in data:
        if(i>0):
            x={"Name":row[0],"Job_Title": row[1],"Birth_Date":row[2],"Birth_Place": row[3]}
            v.append(x)
            
        i=i+1
print(v)

for j in range(len(v)):
    session.run(q1,v[j])
    
list_name.pop(0)
# print(list_name)
related_dict = {"Name":main_node}
for name in list_name:
    related_dict["Related_to"] = name
    session.run(q2,related_dict)
    
print("Done...")