from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_restplus import Api, Resource

from src.utils.general import get_db_credentials

# create Flask app
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = get_db_credentials('conf/local/credentials.yaml')['string']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
api = Api(app)

db = SQLAlchemy(app)


# modelos de base de datos
class Score(db.Model):
    __table_args__ = {'schema': 'api'}
    __tablename__ = 'scores'

    inspection_id = db.Column(db.BigInteger, primary_key=True)
    license_no = db.Column(db.BigInteger)
    score = db.Column(db.Float)
    labels = db.Column(db.Integer)
    threshold = db.Column(db.Float)
    prediction_date = db.Column(db.Date)

    def __repr__(self):
        return f"License: {self.license_no}, score: {self.score}"


# apis
@api.route('/establishment_predictions/<int:id_establishment>')
class GetEstablishmentPredictions(Resource):
    def get(self, id_establishment):
        scores_query = Score.query.filter_by(license_no=id_establishment).order_by(Score.inspection_id.desc()).all()
        predictions = []
        for score in scores_query:
            predictions.append({
                'inspection_id': score.inspection_id,
                'establishment_id': score.license_no,
                'prediction_date': score.prediction_date.strftime("%Y-%m-%d"),
                'score': score.score,
                'label': score.labels,
                'threshold': score.threshold
            })

        return {'predictions': predictions}


@api.route('/date_predictions/<string:prediction_date>')
class GetDatePredictions(Resource):
    def get(self, prediction_date):
        scores_query = Score.query.filter_by(prediction_date=prediction_date).order_by(Score.inspection_id.desc()).all()
        predictions = []
        for score in scores_query:
            predictions.append({
                'inspection_id': score.inspection_id,
                'establishment_id': score.license_no,
                'prediction_date': score.prediction_date.strftime("%Y-%m-%d"),
                'score': score.score,
                'label': score.labels,
                'threshold': score.threshold
            })

        return {'predictions': predictions}


@api.route('/inspection_prediction/<int:inspection_id>')
class GetInspectionPrediction(Resource):
    def get(self, inspection_id):
        score = Score.query.filter_by(inspection_id=inspection_id).first()
        prediction = {
            'inspection_id': score.inspection_id,
            'establishment_id': score.license_no,
            'prediction_date': score.prediction_date.strftime("%Y-%m-%d"),
            'score': score.score,
            'label': score.labels,
            'threshold': score.threshold
        }
        return {'prediction': prediction}


if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0")
