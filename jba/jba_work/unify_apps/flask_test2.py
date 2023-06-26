import requests
# newHeaders = {'Content-type': 'application/json', 'Accept': 'text/plain'}
newHeaders = {'Content-type': 'application/json'}
response = requests.post('http://0.0.0.0:5000/json-example',
                        data={"user": "jba", 
                                "search_words": ["coffeeIsland OR (coffee island)"], 
                                "date_since": "2021-07-10", 
                                "date_until": "2021-07-13",
                                "lang":"en"
                                }, headers = newHeaders)

print("Status code: ", response.status_code)

response_Json = response.json()
print("Printing Post JSON data")
print(response_Json['data'])

print("Content-Type is ", response_Json['headers']['Content-Type'])