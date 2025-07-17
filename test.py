from versionone import V1Meta
import os
from datetime import datetime, timedelta
import json
import httpx

find_statement = {
   "from": "Story", 
  #  "select": [
  #     "Name",
  #     "Scope.Name",
  #     "Owners.Name"
  #   ], 
    "find": "Eli Prater", 
    "findin": ["Owners.Name"]
  }

instance_url = 'https://versionone.usps.gov/v1/rest-1.oauth.v1/query.v1'
token = os.getenv('BEARER_TOKEN')

headers = {"Content-Type": "text/xml;charset=UTF-8"}
headers["Authorization"] = f"Bearer {token}"
client = httpx.Client(headers=headers, verify=False)

response = client.post(instance_url, json=find_statement)
response.raise_for_status()
print(response.request.content)
print("-"*20)
print(response.text)

print("*"*40)
with V1Meta(
  instance_url = 'https://versionone.usps.gov/v1/rest-1.oauth.v1/',
  token = os.getenv('BEARER_TOKEN'),
  verify = False
  ) as v1:

  # stories = v1.Story.filter(f"CreateDate>='{datetime.now() - timedelta(days=30)}'") # internal numeric ID

  try:
      _, stories = v1.server.fetch("/query.v1", postdata=find_statement, dtype='json')
      stories = json.loads(stories)
      for oid in stories[0]:
         story = v1.asset_from_oid(oid.get("_oid"))
         print(story.CreateDate, story.Name)

  except httpx.HTTPStatusError as e:
      print("Error during find:", e)
      