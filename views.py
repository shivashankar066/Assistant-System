import logging
import os
import pickle
import time


from django.conf import settings
from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.views import APIView

from .Integration import Integration
from .ResponseJson import ResponseJson
from .errorcode import ErrorCodes

from configparser import ConfigParser
from .InputRequestValidation import InputRequestValidation
from .PatientData import PatientData
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")


epic_rules_data = pd.read_excel(r"C:/Users/mbv/Desktop/MLProject/CPDS-Lite/PredictionEngine/PredictionEngineService/PredictionEngineServiceApp/config/EPIC_RULES.xlsx")
model_aggregated_data = pd.read_excel(r"C:/Users/mbv/Desktop/MLProject/CPDS-Lite/PredictionEngine/PredictionEngineService/PredictionEngineServiceApp/config/Ml_model_aggregated_data.xlsx")
cat_model = pickle.load(open("C:/Users/mbv/Desktop/MLProject/CPDS-Lite/PredictionEngine/PredictionEngineService/PredictionEngineServiceApp/config/catboost_model1.pkl", "rb"))


# config = ConfigParser()
# config.read("../proxy/config.ini")
# print(config["path"]["ml_data_path"])
log_dir = settings.LOG_DIR
# data = pd.read_excel(config["path"]["ml_data_path"])
# print(data.head())
# print("***************")

log_dir = settings.LOG_DIR

def log_file(self):
    try:
        file_name = "PredictionEngine" + ".log"
        f = open(os.path.join(log_dir, file_name), "r")
        file_contents = f.read()
        f.close()
        return HttpResponse(file_contents, content_type="text/plain")
    except Exception as e:
        return HttpResponse(str(e), content_type="text/plain")


class PredictProcedure(APIView):
    '''
        This View takes two numbers as input and does addition of two numbers
        and provides the sum as output
    '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.errorObj = ErrorCodes()
        self.responseObj = ResponseJson()
        self.s_Name = "Recommend Procedure Service"
        self.status = 0
        self.patient_id = "-1"
        self.recommended_code = {}
        extra = {
            "cls_name": self.__class__.__name__,
        }
        self.logger = logging.LoggerAdapter(self.logger, extra)

    def post(self, request, patientId = None):
        start_time = time.time()
        request_data = request.data
        # TODO:: update the Input request Validation
        input_request_validation_obj = InputRequestValidation()
        print("request_data", request_data)
        try:
            self.logger.info("Predict Procedure Started")

            patient_id = request_data["Patient_ID"]
            rule_engine_recommended_code = request_data["Rule_Engine_Recommended_Code"]

            # latest_app_date = request_data["Latest_Appointment_Date"]

            # TODO:: write pre-processing function and then model execution function.
            patient_df = PatientData().gethistoricalData(patient_id)

            # Passing the single patient df to preprocessing data, preparing data for historically followed procedure code
            aggregated_data_of_one_patient_records = PatientData().preProcessingHistoricalData(patient_df)

            epic_rules = epic_rules_data["Procedure_Code"].value_counts().keys().tolist()
            epic_rules = [str(code) for code in epic_rules]
            final_df, procedure_code_paid_by_the_registered_payor, procedure_code_not_paid_by_the_registered_payor = PatientData().resultantDf(aggregated_data_of_one_patient_records,
                                                                    model_aggregated_data,
                                                                    epic_rules)
            ml_reccemendation,patient_number  = PatientData().DataPreperationAndModelPrediction(final_df, cat_model)
            final_recomendation = Integration().integration(rule_engine_recommended_code, ml_reccemendation, patient_id, patient_number)

            print(final_recomendation)
            self.logger.info(patient_df.head())
        # finally:
        #     print(result)
            end_time = time.time()
            self.status = 200
            self.patient_id = patient_id
            #TODO:: remove hard coded data
            self.recommended_code = final_recomendation

        except Exception as e:
            end_time = time.time()
            self.logger.exception(e)
            self.status = 210

        if self.status == 200:
            status_msg = self.errorObj.SuccessMsg
        else:
            status_msg = self.errorObj.FailureMsg

        response = self.responseObj.response_json_object(
            self.s_Name
            + str(self.errorObj.return_error_message(str(self.status))),
            end_time - start_time,
            self.status,
            status_msg,  str(self.patient_id),
            str(self.recommended_code)
        )
        return Response(response)



