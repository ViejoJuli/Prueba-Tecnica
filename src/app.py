from flask import Flask, request, Response
from flask_pymongo import PyMongo
from bson import json_util
from bson.objectid import ObjectId
from datetime import datetime

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://64.227.13.197:27017/backtest_dev"
mongo = PyMongo(app)


def toDate(dateString):
    return datetime.strptime(dateString, "%Y-%m-%d").date()


# Return users who has minimun 2 doses
@app.route("/problem1", methods=["GET"])
def get_users_with_2_doses():

    consulta = mongo.db.patients.aggregate(
        [
            {
                "$lookup": {
                    "from": "encounters",
                    "let": {"patientId": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$$patientId", "$patient_id"]}}},
                        {"$count": "count"},
                    ],
                    "as": "numVacines",
                }
            },
            {"$unwind": "$numVacines"},
            {"$match": {"numVacines.count": {"$gt": 1}}},
        ]
    )
    response = json_util.dumps(consulta)
    return response


# Return a list with the number of doses applied by day
@app.route("/problem2/<vacuna>", methods=["GET"])
def number_of_dosis_per_day(vacuna):
    pipeline = [
        {"$match": {"vacuna": vacuna}},
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$fecha"}},
                "count": {"$sum": 1},
            }
        },
    ]
    consulta = mongo.db.encounters.aggregate(pipeline)
    response = json_util.dumps(consulta)
    return response


# Return number of dosis in a range of dates
@app.route("/problem3", methods=["GET"])
def get_dose_by_date_range():
    init_date = request.args.get("init_date", type=toDate)
    final_date = request.args.get("final_date", type=toDate)
    pipeline = {"fecha": {"$gte": init_date, "$lt": final_date}}
    consulta = mongo.db.encounters.find(pipeline)
    response = json_util.dumps(consulta)
    return response


if __name__ == "__main__":
    app.run(debug=True)
