import logging
import os
import time

import pandas as pd
from django.conf import settings
from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.views import APIView
from configparser import ConfigParser

from .InputRequestValidation import InputRequestValidation
from .PatientData import PatientData
from .Response import Response
from .errorcode import ErrorCodes

config= ConfigParser()
config.read("../proxy/config.ini")


print(config["path"]["rules_data_path"])
log_dir = settings.LOG_DIR
rules_data = pd.read_csv(config["path"]["rules_data_path"])


# rules_data = pd.read_csv(r"C:\Users\mbv\Desktop\RuleEngine\RuleEngineService\config\EpicRules.csv")

def log_file(self):
    try:
        file_name = "RuleEngine" + ".log"
        f = open(os.path.join(log_dir, file_name), "r")
        file_contents = f.read()
        f.close()
        return HttpResponse(file_contents, content_type="text/plain")
    except Exception as e:
        return HttpResponse(str(e), content_type="text/plain")


class RecommendProcedure(APIView):
    '''
        This View takes two numbers as input and does addition of two numbers
        and provides the sum as output
    '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.errorObj = ErrorCodes()
        self.responseObj = Response()
        self.s_Name = "Recommend Procedure Service"
        self.status = 0
        self.patient_practice = "Adhoc"
        self.patient_id = "-1"
        self.recommended_code = {}
        extra = {
            "cls_name": self.__class__.__name__,
        }
        self.logger = logging.LoggerAdapter(self.logger, extra)

    def post(self, request):
        start_time = time.time()
        request_data = request.data
        # TODO:: update the Input request Validation
        input_request_validation_obj = InputRequestValidation()
        try:
            self.logger.info("recommendation Started")

            patient_id = request_data["Patient_ID"]
            latest_app_date = request_data["Latest_Appointment_Date"]

            # TODO:: read the return value from dict and if -1 then don't process further and return failure message
            result_dict = PatientData().gethistoricaldata(patient_id)
            while True:
                try:
                    if result_dict == "-1":
                        print("patient_id has no past history of data")
                        break
                    else:
                        return result_dict
                except ValueError:
                    print("An exception occurred")
                    continue

            # # Populate Derived columns and return the dataframe
            patient_df1 = PatientData().getderiveddata(result_dict, latest_app_date)

            # for regular user apply the rule engine for recommendation of procedure.

            if patient_df1.loc[0, "Patient_Practice"] != "Adhoc":
                self.patient_practice = "Regular"
                recommended_code = PatientData().recommendation(patient_df1, rules_data)

            else:
                recommended_code = PatientData().allproceducecode(rules_data)

            end_time = time.time()
            self.status = 200
            self.patient_id = patient_id
            self.recommended_code = recommended_code
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
            status_msg, self.patient_practice, self.patient_id,
            self.recommended_code
        )

        return Response(response)
