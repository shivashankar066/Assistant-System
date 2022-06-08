import logging
from datetime import datetime
import pandas as pd
import numpy as np
from django.db import connection
import warnings
warnings.filterwarnings("ignore")

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


    def gethistoricalData(self, patient_id):
        """
        this function will fetch the all records
        for the given patient id
        :param int patient_id:
        :return: a dataframe
        """
        result_dict = {"response_key": "-1"}
        try:

            cursor = connection.cursor()
            cursor.execute("""
            select t1.Service_ID,t1.Patient_ID,t1.Patient_Number,d.IMREDEM_CODE,
            t3.patient_age, t1.Actual_Dr_Name,t1.Place_of_Service_Abbr,t1.Proc_Category_Abbr,
            t1.Type_of_Service_Abbr,t3.patient_zip_code,t3.patient_sex,t1.Original_Carrier_Name, t3.Patient_City,
            t3.Patient_State, t5.Diagnosis_Code, t5.Diagnosis_Descr,t2.CoPayment,t2.CoInsurance,
            t1.Primary_Diagnosis_Code,t1.Procedure_Code,t1.Service_Units,
            convert(Date, t1.Service_Date_From) as Service_Date_From, t1.Claim_Number,
            convert(Date, t1.Original_Billing_Date) as Original_Billing_Date,Convert(Date, t2.Date_Paid) as Date_Paid,
            t1.Service_Fee,t2.Amount, t2.Allowed, t2.Deductible, t2.Transaction_Type, t4.Abbreviation , 
            t4.Description, t4.Self_Pay_TranCode
            from  vwGenSvcInfo as t1 left join vwGenSvcPmtInfo as t2
            on t1.Service_ID = t2.Service_ID 
            inner join
            vwGenSvcDiagInfo as t5 on t1.Service_ID = t5.Service_ID
            inner join
            [EMR].[HPSITE].[DEMOGRAPHICS_VIEW] as d on t1.Patient_Number = d.DEM_EXTERNALID
            inner join vwGenPatInfo as t3 on t1.Patient_Number = t3.Patient_Number
            inner join [dbo].[vUAI_Transaction_Codes] as t4 on t2.Transaction_Code_Abbr=t4.Abbreviation
            left join  dbo.[vUAI_Appointments] as t6 on t1.Voucher_Number = t6.Encounter_Number
            where  t1.Primary_Diagnosis_Code between 'E08' and 'E13' and
             t5.Diagnosis_Code between 'E08' and 'E13' and t1.Procedure_Code In 
            ('85018','3046F','81000','82043','80053','80061','84443','95250','95251',
            '2028F','93922','92250','93224','80048') and t1.Patient_ID= %s """, (patient_id,))

            result = pd.DataFrame([list(elem) for elem in cursor.fetchall()])
            result.columns = ["Service_ID","Patient_ID","Patient_Number","IMREDEM_CODE",
                              "patient_age", "Actual_Dr_Name","Place_of_Service_Abbr","Proc_Category_Abbr",
                              "Type_of_Service_Abbr","patient_zip_code","patient_sex","Original_Carrier_Name",
                              "Patient_City","Patient_State", "Diagnosis_Code","Diagnosis_Descr",
                              "CoPayment","CoInsurance","Primary_Diagnosis_Code","Procedure_Code",
                              "Service_Units","Service_Date_From", "Claim_Number",
                              "Original_Billing_Date","Date_Paid","Service_Fee","Amount", "Allowed",
                              "Deductible", "Transaction_Type", "Abbreviation", "Description", "Self_Pay_TranCode"]

            print("result", result)
            self.logger.info("result == " + str(result))

            if result.empty:
                result = {"no record found"}
            else:
                print("result == ", result)

        except Exception as e:
            print(e)
            result = {"failed "}
            # finally:
            #     connection.close()
        return result

    def preProcessingHistoricalData(self,one_patient_data):
        """
                This function returns the one patientof preprocecessed data
                :param patient_id:
                :return: return aggregation of preprocessed data of the patient in dataframe
                """
        global final_df
        try :
            one_patient_data["Description"] = one_patient_data["Description"].apply(
                lambda x: -999 if str(x).startswith("Self") else x)
            self_pay_index = one_patient_data.loc[one_patient_data["Description"] == -999].index
            one_patient_data = one_patient_data.drop(self_pay_index)

            list_of_col_drop = ["Self Pay Transfer", "Self Pay Adjustment", "Self Pay Financial Hardship"]
            one_patient_data = one_patient_data[~one_patient_data.Description.isin(list_of_col_drop)]

            # drop IMREDEM_COE and Patient_ID keep only Patient_Number
            done_patient_data = one_patient_data.drop(["IMREDEM_CODE", "Patient_ID"], axis=1)

            # Preprocessing

            one_patient_data = one_patient_data.drop_duplicates() #
            one_patient_data = one_patient_data.loc[one_patient_data["Service_Fee"] > 0]
            one_patient_data = one_patient_data.loc[one_patient_data["Original_Billing_Date"].notna()]
            one_patient_data = one_patient_data.loc[one_patient_data["Date_Paid"].notna()]
            index_names = one_patient_data[(one_patient_data['Transaction_Type'] == 'T') & (one_patient_data['Amount']> 0)].index
            one_patient_data.drop(index_names, inplace = True)

            x = one_patient_data.groupby(["Service_ID"], as_index=False).agg({

            "Patient_Number" : "first",
            'patient_age': "max",
            'Actual_Dr_Name':'first',
            'Place_of_Service_Abbr':'first',
            'Proc_Category_Abbr':'first',
            'Type_of_Service_Abbr':'first',
            'patient_zip_code': "first",
            'patient_sex' : "first",
            'Original_Carrier_Name':"first",
            'Patient_City':"first",
            'Patient_State':"first",
            "Diagnosis_Code" : 'first',
            "Diagnosis_Descr":"first",
            'CoInsurance':"sum",
            'CoPayment':"sum",
            "Primary_Diagnosis_Code" : "first",
            "Procedure_Code":"first",
            'Service_Units':"sum",
            'Service_Date_From':"first",
            "Claim_Number" : "first",
            "Original_Billing_Date":"first",
            "Date_Paid":'last',
            "Service_Fee":"max",
            "Amount":"max",
            'Allowed': 'max',
            "Deductible":"max",
            "Transaction_Type":"count",
            })

            final_df = x.groupby(["Patient_Number", "Original_Carrier_Name","Primary_Diagnosis_Code","Procedure_Code"],
                                 as_index=False).agg({

            "Service_ID" : "first",
        #     "Patient_Number" : "first",
            'patient_age': "max",
            'Actual_Dr_Name':'first',
            'Place_of_Service_Abbr':'first',
            'Proc_Category_Abbr':'first',
            'Type_of_Service_Abbr':'first',
            'patient_zip_code': "first",
            'patient_sex' : "first",
        #     'Original_Carrier_Name':"first",
            'Patient_City':"first",
            'Patient_State':"first",
            "Diagnosis_Code" : 'first',
            "Diagnosis_Descr":"first",
            'CoInsurance':"sum",
            'CoPayment':"sum",
        #     "Primary_Diagnosis_Code" : "first",
        #     "Procedure_Code":"first",
            'Service_Units':"sum",
            'Service_Date_From':"first",
            "Claim_Number" : "first",
            "Original_Billing_Date":"first",
            "Date_Paid":'last',
            "Service_Fee":"max",
            "Amount":"max",
            'Allowed': 'max',
            "Deductible":"max",
            "Transaction_Type":"count",

        })

            return final_df

        except Exception as e:
            self.logger.exception(str(e))


    def patientProcedureCodeFollwedAndNotFollowed(self, history_records_followed, ml_model_aggregated_data,
                                                  client_procedure_code):
        """

        :param history_records_followed:
        :param ml_model_aggregated_data:
        :param client_procedure_code:
        :return:
        """

        try:
            records_followed = history_records_followed
            procedure_code_not_followed = dict()

            patient_id = records_followed["Patient_Number"][0]
            followed_procedure_code_list = records_followed["Procedure_Code"].value_counts().keys().tolist()

            not_followed_procedure_code = []

            for proc_code in client_procedure_code:
                if proc_code not in followed_procedure_code_list:
                    not_followed_procedure_code.append(proc_code)

            procedure_code_not_followed[patient_id] = not_followed_procedure_code

            patient_age = records_followed["patient_age"].iloc[0]
            patient_zip_code = records_followed["patient_zip_code"].iloc[0]

            return patient_id, patient_age, patient_zip_code, records_followed, procedure_code_not_followed
        except Exception as e:
            self.logger.exception(str(e))



    def resultantDf(self, final_data, ml_model_aggregated_data, client_procedure_code):
        """

        :param final_data:
        :param ml_model_aggregated_data:
        :param client_procedure_code:
        :return:
        """

        patient_id,patient_age, patient_zip_code, records_followed, procedure_code_not_followed = self.patientProcedureCodeFollwedAndNotFollowed(final_data,
                                                                                                                   ml_model_aggregated_data,
                                                                                                                   client_procedure_code)

        list_of_columns_to_keep = ['Patient_Number', 'Original_Carrier_Name', 'Primary_Diagnosis_Code',
                                   'Procedure_Code',
                                   'patient_age', 'Actual_Dr_Name', 'Place_of_Service_Abbr',
                                   'Proc_Category_Abbr', 'Type_of_Service_Abbr', 'patient_zip_code', 'patient_sex',
                                   'Patient_City', 'Patient_State', 'Diagnosis_Code', 'Diagnosis_Descr', 'CoInsurance',
                                   'CoPayment', 'Service_Units', 'Service_Date_From', 'Claim_Number',
                                   'Original_Billing_Date',
                                   'Date_Paid', 'Service_Fee', 'Amount', 'Allowed', 'Deductible', 'Transaction_Type']
        try:


            # reading data from the final dataset, Either can execute the query for these aggregated data are
            # of 5 years records so reading it in excel
            list_of_columns_to_keep_df = ml_model_aggregated_data[list_of_columns_to_keep]

            # drop the duplicates
            list_of_columns_to_keep_df = list_of_columns_to_keep_df.drop_duplicates()

            # Remove the white trailing spaces
            list_of_columns_to_keep_df["Primary_Diagnosis_Code"] = list_of_columns_to_keep_df["Primary_Diagnosis_Code"].str.strip()

            # Collect the unique payor name list
            records_followed_payor_name = records_followed["Original_Carrier_Name"].value_counts().keys().tolist()

            # Collect the unique primary diagnosis code in list
            records_followed_primary_diag_name = records_followed["Primary_Diagnosis_Code"].str.strip().value_counts().keys().tolist()


            # Collect the data w.r.t to the payor where one patient is registered(filtering the data w.r.t payor)
            resultant_df = pd.DataFrame()
            for rec in records_followed_payor_name:
                res = list_of_columns_to_keep_df.loc[list_of_columns_to_keep_df["Original_Carrier_Name"] == rec]
                resultant_df = resultant_df.append(res)

            # filterring the data w.r.to diagnosis code where the patient is suffered
            result_df = pd.DataFrame()
            for rec in records_followed_primary_diag_name:
                res = resultant_df.loc[resultant_df["Primary_Diagnosis_Code"] == rec]
                result_df = result_df.append(res)

            # List of procedure code where in the history not perfomed by the patient
            list_of_procedure_need_to_be_add = procedure_code_not_followed[patient_id]

            # dataframe creation where the procedure code not performed
            semi_final_df = pd.DataFrame()
            for proc in list_of_procedure_need_to_be_add:
                res = result_df.loc[result_df["Procedure_Code"] == proc]
                semi_final_df = semi_final_df.append(res)

            # Procedure code paid by the payor
            procedure_code_paid_by_the_registered_payor = semi_final_df["Procedure_Code"].value_counts().keys().tolist()
            # procedure code not paid by the payor
            procedure_code_not_paid_by_the_registered_payor = []

            for proc in list_of_procedure_need_to_be_add:
                if proc not in procedure_code_paid_by_the_registered_payor:
                    procedure_code_not_paid_by_the_registered_payor.append(proc)

            # aggregation required to create a uniqueness of the data records
            semi_final_df = semi_final_df.reset_index(drop=True)
            semi_final_df = semi_final_df.groupby(["Original_Carrier_Name", "Primary_Diagnosis_Code", "Procedure_Code"]).agg({


            'patient_age': "max",
            'Actual_Dr_Name':'first',
            'Place_of_Service_Abbr':'first',
            'Proc_Category_Abbr':'first',
            'Type_of_Service_Abbr':'first',
            'patient_zip_code': "first",
            'patient_sex' : "first",
            'Patient_City':"first",
            'Patient_State':"first",
            "Diagnosis_Code" : 'first',
            "Diagnosis_Descr":"first",
            'CoInsurance':"sum",
            'CoPayment':"sum",
            'Service_Units':"sum",
            'Service_Date_From':"first",
            "Claim_Number" : "first",
            "Original_Billing_Date":"first",
            "Date_Paid":'last',
            "Service_Fee":"max",
            "Amount":"max",
            'Allowed': 'max',
            "Deductible":"max",
            "Transaction_Type":"count",

                }).reset_index()

            # Adding the columns where the attributes are required for ML model
            semi_final_df["Patient_Number"] = patient_id
            semi_final_df["patient_age"] = patient_age
            semi_final_df["patient_zip_code"] = patient_zip_code

            # Columns need for creating input dataset to the model
            columns_needed_for_df = list_of_columns_to_keep

            records_followed = records_followed[columns_needed_for_df]

            # Rearrange the columns as per the ML model input
            semi_final_df = semi_final_df.loc[:,['Patient_Number','Original_Carrier_Name','Primary_Diagnosis_Code','Procedure_Code',
                                       'patient_age','Actual_Dr_Name','Place_of_Service_Abbr',
                                       'Proc_Category_Abbr','Type_of_Service_Abbr','patient_zip_code','patient_sex',
                                       'Patient_City','Patient_State','Diagnosis_Code','Diagnosis_Descr','CoInsurance',
                                       'CoPayment','Service_Units','Service_Date_From','Claim_Number','Original_Billing_Date',
                                       'Date_Paid','Service_Fee','Amount','Allowed','Deductible','Transaction_Type']]

            records_followed = records_followed.reset_index(drop=True)
            semi_final_df = semi_final_df.reset_index(drop=True)
            final_df = records_followed.append(semi_final_df)
            final_df = final_df.reset_index(drop=True)

            return final_df, procedure_code_paid_by_the_registered_payor, procedure_code_not_paid_by_the_registered_payor

        except Exception as e:
            self.logger.exception(str(e))




    def DataPreperationAndModelPrediction(self,df, model):
        """

        :param df:
        :param model:
        :return:
        """
        ml_reccomenation = dict()
        try:

            df["Patient_Number"] = df["Patient_Number"].astype("category")
            df["Original_Carrier_Name"] = df["Original_Carrier_Name"].astype("category")
            df["Primary_Diagnosis_Code"] = df["Primary_Diagnosis_Code"].astype("category")
            df["Procedure_Code"] = df["Procedure_Code"].astype("category")

            df["Actual_Dr_Name"] = df["Actual_Dr_Name"].astype("category")
            df["Place_of_Service_Abbr"] = df["Place_of_Service_Abbr"].astype("category")
            df["Proc_Category_Abbr"] = df["Proc_Category_Abbr"].astype("category")
            df["Type_of_Service_Abbr"] = df["Type_of_Service_Abbr"].astype("category")
            df["patient_zip_code"] = df["patient_zip_code"].astype("category")
            df["patient_sex"] = df["patient_sex"].astype("category")
            df["Patient_City"] = df["Patient_City"].astype("category")
            df["Patient_State"] = df["Patient_State"].astype("category")
            df["Diagnosis_Code"] = df["Diagnosis_Code"].astype("category")
            df["Diagnosis_Descr"] = df["Diagnosis_Descr"].astype("category")
            df["Service_Date_From"] = pd.to_datetime(df["Service_Date_From"])
            df["Claim_Number"] = df["Claim_Number"].astype("category")
            df["Original_Billing_Date"] = pd.to_datetime(df["Original_Billing_Date"])
            df["Date_Paid"] = pd.to_datetime(df["Date_Paid"])


            # Drop deductible column
            df = df.drop("Deductible", axis=1)
            # only one record was found were co insurance is less than 0
            df = df[df["CoInsurance"] >= 0]


            # Create a new feature
            df["Payment_Gap_In_Days"] = df["Date_Paid"] - df["Original_Billing_Date"]
            df["Payment_Gap_In_Days"] = df["Payment_Gap_In_Days"].apply(lambda x : int(str(x).split(" ")[0]))

            # Drop These features
            df = df.drop(["Date_Paid", "Original_Billing_Date", "Service_Date_From"], axis=1)

            # Take the payment gap greater than 0 days
            df = df[df["Payment_Gap_In_Days"] > 0]

             # Fill the allowed column to be 0
            df["Allowed"] = df["Allowed"].fillna(0)


            # convert the data types
            df["Allowed"] = df["Allowed"].astype("float64")
            df["Service_Fee"] = df["Service_Fee"].astype("float64")
            # Derive the new feature payment portion percentage
            df["Payment_portion_percentage"] = ((df["Allowed"] / df["Service_Fee"]) * 100)

            prediction = model.predict(df)
            prediction = np.round(prediction, 2)
            df["Predicted_Score"] = pd.Series(prediction, name="Predicted_Score")
            df = df.reset_index(drop = True)

            df = df.sort_values(["Predicted_Score"], ascending=False)

            result = df.loc[:,["Procedure_Code","Predicted_Score"]]
            result = result.loc[result["Predicted_Score"] >= 0]

            print(result)
            patient_number = df.loc[:,"Patient_Number"][0]
            res_dict = dict()
            for col in result.columns:
                res_dict[col] = result[col].values.tolist()
            ml_reccomenation[patient_number] = res_dict

        except Exception as e:
            self.logger.exception(str(e))

        return ml_reccomenation, patient_number



