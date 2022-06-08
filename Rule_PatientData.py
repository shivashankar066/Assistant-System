import logging
from datetime import datetime

import pandas as pd
from django.db import connection
import pandas as pd

format = '%Y-%m-%d'
logger = logging.getLogger(__name__)


class PatientData:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.sName = "Recommend Procedure Service"
        self.status = 0
        extra = {
            "cls_name": self.__class__.__name__,
        }
        self.logger = logging.LoggerAdapter(self.logger, extra)

    def gethistoricaldata(self, patient_id):
        """
        this function will fetch the all records
        for the given patient id
        :param int patient_id:
        :return: a dataframe
        """
        #result_dict = {"response_key": "-1"}
        try:

            cursor = connection.cursor()
            cursor.execute("""Select t1.[Patient_ID],t1.[Service_Date_To],t1.[Service_Date_To],
                           t1.[Voucher_Number],t1.[Procedure_Code],
                           t1.[Primary_Diagnosis_Code],t2.[Appointment_DateTime],t2.[Encounter_Number]
                           from [vwGenSvcInfo] as t1
                           inner join [vUAI_Appointments] as t2  on 
                           t1.Voucher_Number = t2.Encounter_Number where Primary_Diagnosis_Code
                           between 'E08' and 'E13' and Procedure_Code In 
                           ('85018','3046F','81000','82043','80053','80061','84443','95250','95251',
                           '2028F','93922','92250','93224','80048')and t1.Patient_ID = %s """,(patient_id,))

            result = pd.DataFrame([list(elem) for elem in cursor.fetchall()])
            result.columns = ['Patient_ID', 'Service_Date_To', 'Service_Date_From', 'Voucher_Number',
                              'Procedure_Code', 'Primary_Diagnosis_Code', 'Appointment_DateTime', 'Encounter_Number']

            self.logger.info("result == " + str(result))

            if result.empty:
                result_dict = {"response_key": "-1"}
            else:
                result_dict = {"response_key": result}

        except Exception as e:
            self.logger.exception(str(e))
            result_dict = {"response_key": "-1"}

        return result_dict

    def getderiveddata(self, actual_data, latest_app_date):
        try:

            app_date = datetime.strptime(latest_app_date, format)
            actual_data["new_date"] = app_date
            actual_data = actual_data.rename(columns={"Appointment_DateTime": "Appointment_Date"})
            actual_data["Service_Date_max"] = actual_data.groupby("Patient_ID")["Service_Date_From"].transform(max)

            actual_data["last_visit_day"] = (actual_data["new_date"] - actual_data["Service_Date_max"])
            actual_data["last_visit_day"] = actual_data["last_visit_day"].apply(lambda x: int(str(x).split(" ")[0]))
            actual_data["Patient_Practice"] = actual_data.loc[:, "last_visit_day"].apply(
                lambda x: "Adhoc" if x >= 90 else "Regular")

            actual_data["Appointment_Date"] = pd.to_datetime(actual_data["Appointment_Date"])
            actual_data["Diff_in_days"] = (actual_data["new_date"] - actual_data["Service_Date_From"])
            actual_data["Diff_in_days"] = actual_data["Diff_in_days"].apply(lambda x: int(str(x).split(" ")[0]))
            print("patient_df1:", actual_data)
        except Exception as e:
            self.logger.exception(str(e))

        return actual_data

    def recommendation(self, one_patient_record, rules_data):
        try:
            recommendation_procedure_list = {}

            unique_procedure_code = rules_data["Procedure_Code"].value_counts().keys().tolist()

            for row in range(len(one_patient_record)):
                procedure_code_list = []
                diff_in_days_list = []

                procedure_code_list.append(one_patient_record.loc[row, "Procedure_Code"])
                diff_in_days_list.append(one_patient_record.loc[row, "Diff_in_days"])

                for record in range(len(procedure_code_list)):
                    procedure_code = procedure_code_list[record]
                    diff_in_days = diff_in_days_list[record]

                    if procedure_code in unique_procedure_code:
                        res = rules_data.loc[rules_data["Procedure_Code"] == procedure_code]
                        rules_procedure_code = res["Procedure_Code"].values[0]
                        rules_diff_in_days = res["Freq_Threshold_In_Days"].values[0]

                        if rules_procedure_code == procedure_code and int(rules_diff_in_days) <= (diff_in_days):
                            recommendation_procedure_list[procedure_code] = "1"
                        else:
                            recommendation_procedure_list[procedure_code] = "0"

                    else:
                        for procedure in unique_procedure_code:
                            if procedure not in list(recommendation_procedure_list.keys()):
                                recommendation_procedure_list[procedure] = "1"

        except Exception as e:
            self.logger.exception(str(e))

        return recommendation_procedure_list

    def allproceducecode(self, rules_data):

        rec_result = dict()
        Standard_procedure_List = rules_data["Procedure_Code"].value_counts().keys().tolist()
        for item in Standard_procedure_List:
            rec_result[item] = "1"
        # return {str(Patient_ID): rec_result}
        return rec_result

