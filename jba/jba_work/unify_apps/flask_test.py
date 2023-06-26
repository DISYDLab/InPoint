# import main Flask class and request object
from flask import Flask, request, jsonify
import requests

# create the Flask app
app = Flask(__name__)

@app.route('/query-example')
def query_example():
    # if key doesn't exist, returns None
    language = request.args.get('language')

    return '''<h1>The language value is: {}</h1>'''.format(language)

# allow both GET and POST requests
@app.route('/form-example', methods=['GET', 'POST'])
def form_example():
    # handle the POST request
    if request.method == 'POST':
        myquery = request.form.get('myquery')
        return '''
                  <h1>The myquery value is: {}</h1>'''.format(myquery)
        # otherwise handle the GET request
    return '''
                 <form method="POST">
                     <div><label>Query: <input type="text" name="myquery"></label></div>
                     <input type="submit" value="Submit">
                 </form>'''

@app.route('/json-example')
def json_example():
    # newHeaders = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    # response = requests.post('http://0.0.0.0:5000/json-example',
    #                         data={"user": "jba", 
    # "search_words": ["coffeeIsland OR (coffee island)"], 
    # "date_since": "2021-07-10", 
    # "date_until": "2021-07-13",
    # "lang":"en"
    # }, headers = newHeaders)

    # print("Status code: ", response.status_code)

    # response_Json = response.json()
    # print("Printing Post JSON data")
    # print(response_Json['data'])
    response_Json = request.get_json()
    print("Content-Type is ", response_Json['headers']['Content-Type'])
    ret = jsonify({'result': response_Json})
    return {"message": ret}, 202

if __name__ == '__main__':
    # run app in debug mode on port 5000
    # app.run(debug=True, port=5001)
    # multiprocessing.set_start_method('spawn')
    app.run(host='0.0.0.0', port=5001, threaded=True)

