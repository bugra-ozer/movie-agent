import flask

app=flask.Flask(__name__)
@app.route("/")
def x():
    return 'hello world'

if __name__ == "__main__":
    app.run(debug=True)