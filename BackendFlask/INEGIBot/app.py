from random import random
from flask import Flask, send_file
from flask_cors import CORS
from spark_nacional import *

app = Flask(__name__)
CORS(app)


@app.route('/message/<message>', methods = ['GET'])
def hello_world(message):
    random_number = int(random()*3)
    
    print(message)
    
    messages = [
        {
            "type": "simple",
            "message":"En 2020 la poblacion de Monterrey era de 5,784,442 personas"
            },
        {
            "type": "simple",
            "message":"En 2020 la poblacion de mujeres en Monterrey era de 2,893,492"
            },
        {
            "type": "graph_1",
            "message":"En 2020 la poblacion de hombres en Monterrey era de 2,890,950 y la de mujeres era de 2,893,492",
            "graph": {
                "labels":["Hombres", "Mujeres"],
                "data":[2890950, 2893492]
                }
            }
    ]
    
    return messages[random_number]
    
if __name__ == '__main__':
    app.run(debug=True)