import requests
import xml.etree.ElementTree as ET
import pandas as pd
from dotenv import load_dotenv
import os
import boto3
from io import StringIO
#from datetime import datetime

def parseXML(response):
    # converter the byte p/ string
    xml_str = response.content.decode('utf-8')

    # converter p/ objecto XML
    root = ET.fromstring(xml_str)

    # Define namespaces to handle the prefixed tags
    namespaces = {
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
        'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
        'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic'
    }

    '''
    # Extract the header information
    header = root.find('message:Header', namespaces)
    message_id = header.find('message:ID', namespaces).text
    test = header.find('message:Test', namespaces).text
    prepared = header.find('message:Prepared', namespaces).text
    sender_id = header.find('message:Sender', namespaces).attrib['id']
    structure_id = header.find('message:Structure', namespaces).attrib['structureID']
    dataset_action = header.find('message:DataSetAction', namespaces).text
    dataset_id = header.find('message:DataSetID', namespaces).text

    '''    
    # Extract all <generic:Series> elements
    series_list = root.findall('.//generic:Series', namespaces)

    # Initialize a list to hold extracted data
    data = []

    # Iterate over each series
    for series in series_list:
        freq = series.find('.//generic:SeriesKey/generic:Value[@id="FREQ"]', namespaces).get('value')
        ref_area = series.find('.//generic:SeriesKey/generic:Value[@id="REF_AREA"]', namespaces).get('value')
        commodity = series.find('.//generic:SeriesKey/generic:Value[@id="COMMODITY"]', namespaces).get('value')
        transaction = series.find('.//generic:SeriesKey/generic:Value[@id="TRANSACTION"]', namespaces).get('value')
        unit_measure = series.find('.//generic:Attributes/generic:Value[@id="UNIT_MEASURE"]', namespaces).get('value')
        # Iterate over each <generic:Obs> element within the current series
        for obs in series.findall('.//generic:Obs', namespaces):
            # Extract the time period and value
            time_period = obs.find('.//generic:ObsDimension', namespaces).get('value')
            value = obs.find('.//generic:ObsValue', namespaces).get('value')
            unit_mult = obs.find('.//generic:Attributes/generic:Value[@id="UNIT_MULT"]', namespaces).get('value')
            obs_status = obs.find('.//generic:Attributes/generic:Value[@id="OBS_STATUS"]', namespaces).get('value')
            conversion_factor = obs.find('.//generic:Attributes/generic:Value[@id="CONVERSION_FACTOR"]', namespaces).get('value')
            # Append to data list
            data.append((freq, ref_area, commodity, transaction, unit_measure, time_period, value, unit_mult, obs_status, conversion_factor))

    return data

def saveS3(fx, region_name, access_key, secret_access_key, bucket_name):
        # converter o DataFrame para CSV no buffer de string
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # NM 28.05.2024 activei versionamento no S3 (o prefixo data já não é preciso)
        #file_name = datetime.today().strftime('%Y%m%d') + "_" + fx + ".csv"
        file_name = fx + ".csv"

        # gravar no bucket do S3
        s3_client = boto3.client('s3', region_name=region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

        return True

if __name__ == "__main__":

    un_data_api_endpoints = {
        "total_electricity_consumed": "https://data.un.org/ws/rest/data/UNSD,DF_UNDATA_ENERGY,1.1/.004+008+012+016+020+024+028+031+032+036+040+044+048+050+051+052+056+060+064+068+070+072+076+081+084+090+092+096+100+104+108+112+116+120+124+132+136+140+144+148+152+156+158+170+174+175+178+180+184+188+191+192+196+200+203+204+208+212+214+218+222+226+230+231+232+233+234+238+242+246+251+254+258+262+266+268+270+275+276+278+280+288+292+296+300+304+308+312+316+320+324+328+332+340+344+348+352+356+360+364+368+372+376+382+384+388+392+398+400+404+408+410+412+414+417+418+422+426+428+430+434+438+440+442+446+450+454+458+462+466+470+474+478+480+484+496+498+499+500+504+508+512+516+520+524+528+530+531+533+534+535+540+548+554+558+562+566+570+579+580+582+583+584+585+586+591+598+600+604+608+616+620+624+626+630+634+638+642+643+646+654+659+660+662+666+670+678+682+686+688+690+694+702+703+704+705+706+710+716+720+724+728+729+736+740+748+752+757+760+762+764+768+776+780+784+788+792+795+796+798+800+804+807+810+818+826+831+832+833+834+840+850+854+858+860+862+876+882+886+887+890+891+894.7000.12/ALL/?detail=full&startPeriod=1990-01-01&endPeriod=2022-12-31&dimensionAtObservation=TIME_PERIOD",
        "renewables_electricity_production": "https://data.un.org/ws/rest/data/UNSD,DF_UNDATA_ENERGY,1.1/.004+008+012+016+020+024+028+031+032+036+040+044+048+050+051+052+056+060+064+068+070+072+076+081+084+090+092+096+100+104+108+112+116+120+124+132+136+140+144+148+152+156+158+170+174+175+178+180+184+188+191+192+196+200+203+204+208+212+214+218+222+226+230+231+232+233+234+238+242+246+251+254+258+262+266+268+270+275+276+278+280+288+292+296+300+304+308+312+316+320+324+328+332+340+344+348+352+356+360+364+368+372+376+382+384+388+392+398+400+404+408+410+412+414+417+418+422+426+428+430+434+438+440+442+446+450+454+458+462+466+470+474+478+480+484+496+498+499+500+504+508+512+516+520+524+528+530+531+533+534+535+540+548+554+558+562+566+570+579+580+582+583+584+585+586+591+598+600+604+608+616+620+624+626+630+634+638+642+643+646+654+659+660+662+666+670+678+682+686+688+690+694+702+703+704+705+706+710+716+720+724+728+729+736+740+748+752+757+760+762+764+768+776+780+784+788+792+795+796+798+800+804+807+810+818+826+831+832+833+834+840+850+854+858+860+862+876+882+886+887+890+891+894.7000G+7000H+7000O+7000S+7000W./ALL/?detail=full&startPeriod=1990-01-01&endPeriod=2022-12-31&dimensionAtObservation=TIME_PERIOD"
    }
    
    load_dotenv()

    region_name = os.getenv("REGION_NAME")
    bucket_name = os.getenv("BUCKET_NAME")
    access_key = os.getenv("ACCESS_KEY")
    secret_access_key = os.getenv("SECRET_ACCESS_KEY")
    
    for key, value in un_data_api_endpoints.items():
        # fazer pedido GET
        response = requests.get(value)

        # se pedido OK
        if response.status_code == 200:
            df = pd.DataFrame(parseXML(response), columns=['freq', 'ref_area', 'commodity', 'transaction', 'unit_measure', 'time_period', 'value', 'unit_mult', 'obs_status', 'conversion_factor'])
          
            #print(df)

            # gravar S3
            rs = False
            rs = saveS3(key, region_name, access_key, secret_access_key, bucket_name)

            if rs:
                print(f"Arquivo {key} foi gravado no bucket {bucket_name}.")

        else:
            print("*** Pedido GET falhou! ***")







