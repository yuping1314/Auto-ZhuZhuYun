"""
user-agettest-
Version：
Author:yuping
Date:2023/1/19
"""
# from fake_useragent import UserAgent
import requests

url = "http://d2g6u4gh6d9rq0.cloudfront.net/browsers/fake_useragent_0.1.10.json"

response = requests.get(url)

with open("user_agent.json", 'w') as fp:
    fp.write(response.text)

