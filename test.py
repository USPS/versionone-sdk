from v1pysdk import V1Meta
import os

with V1Meta(
  instance_url = 'https://versionone.usps.gov/v1/rest-1.oauth.v1/',
  token = os.getenv('BEARER_TOKEN')
  ) as v1:

  stories = v1.Story.filter("CreateDate>='2025-07-16'").select("Name") # internal numeric ID

  for story in stories:
      print(story.CreateDate, story.Name)