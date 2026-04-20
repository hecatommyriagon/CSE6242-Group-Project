import logging
from flask import Flask, render_template
from src.frontend_utils import get_data
from src.data_api import data_api

logging.basicConfig(level=logging.DEBUG)


app = Flask(__name__)
app.register_blueprint(data_api)


@app.route("/")
def home():
    logging.info(f"home()")
    return render_template("index.html")


@app.route("/map")
def map_page():
    logging.info("map_page()")
    # Optionally, you can remove get_data() if the UI will fetch via API
    data = get_data()
    return render_template("map.html", data=data)


if __name__ == "__main__":
    app.run(debug=True)
