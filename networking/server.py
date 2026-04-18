from flask import request, Flask, jsonify
from main import MovieService
import os
from pathlib import Path

os.chdir(Path(__file__).parent.parent)

app=Flask(__name__)
service=MovieService()
@app.route("/recommendations", methods=['POST'])

def Service():
    text = request.get_json(force=True)
    filter_tools = text['filter_tools']
    response=service.recommend(filter_tools)
    response=jsonify(response)
    return response

if __name__ == "__main__":
    app.run(debug=False)