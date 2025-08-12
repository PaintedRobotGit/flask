from flask import Flask, jsonify
from validation_ai import validation_ai_bp
import os

app = Flask(__name__)
app.register_blueprint(validation_ai_bp)


@app.route('/')
def index():
    return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})


if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))
