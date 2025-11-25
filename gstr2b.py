import requests
import os
from datetime import datetime,timedelta
import base64
import wget
import json
from pathlib import Path
import tarfile
import pandas as pd
import psycopg2
import shutil
import sys
import io
import multiprocessing
import argparse
import pandas as pd
import json
import numpy as np
import time
url1="https://boapi.internal.gst.gov.in/govtapi/v0.2/authenticate"
url2="https://boapi.internal.gst.gov.in/govtapi/v2.0/common?action=FILECNT&type=R2B&state_cd=01&date="
header1={"clientid":"l7xx1770d53ccd16417c9252ba7ecde54b69",
"client-secret":"d54330e9f8c743db8772770bba1cc019",
"username":"GSTG2G01",
'state-cd':"01",
"Content-Type":"application/json"}
body1={"action":"ACCESSTOKEN","username":"GSTG2G01","password":"bsMWXnnIMSIA1AsZo9L/4BHaoJc5hmGEnKhA+IfeILAaMUbbbdd1K55ZPxU05RPB7xekYyGT3gy3F0JCKlAIOJGwZPIOr1zoCRilpu27uMqrd4a6qRoeOwQQIOzyLXbvl0usfWU/1ydWZjVJPq/rMUKcJIWaoIuNAijskXqGmcqVLX6D57qUnevj6UC9hVzQu+04gRza/CTyGLMJtYwCLFBlBVSZTDbd6grlWtn8FBB+x+FIAIEAMxijn3JRcIlFPyTkBpCroq3im2uo6tr9u82bzL7GaI8WJOf8SmOD8zrxf2ExH4e5w2eeZE3RMnUpFz7i3rmdaGKu8jNkptdiaQ==","app_key":"D4x/7LnmC8sMWqnxRwuWr7+fcipumcGN9N4ut98bePt/QVty66STc5gsjjtx26TusFYaRaEmaXwXPWaRxzeCWY1wqTYtb7G8GXzf2Dr+mDTCGGNgrR2BtyXufA+VFHyhJX1mwPolHYh8hqlEJk75SkRDfT7f8YSdgt1SkLiaWIMygplcEOvvWknNKzXzkTf+hzj5DaHgzGGRuYne7drloyolRqo9V6j68C9q0T1l2Oh/kVQaa6cFdTxmfSPVRb2WBYzu6KJhKNHqpCDrQ1EESgEs+zffDfzJTDKlyFz/QgnGGJ5/mDfJPJjDzj1ip9eC4yAedQBW4tTbof9PQui90Q=="}
header2={"clientid":"l7xx1770d53ccd16417c9252ba7ecde54b69",
"client-secret":"d54330e9f8c743db8772770bba1cc019",
"username":"GSTG2G01",
'state-cd':"01",
"Content-Type":"application/json"
}



pt=Path("G:/Daily json data/R2B")

from datetime import datetime, timedelta
def parse(file):
    # final parsing code


    f = open(file)
    data = json.load(f)
    #1
    try:
        data_import_date=datetime.now().date()
        data_import_date=data_import_date.strftime("%Y-%m-%d")
        
    except:
        data_import_date="No date"

    try:
        gstin=data["gstin"]
    except:
        gstin=""
    
    Financial_year=""
    try:
        rtnprd=data["rtnprd"]
        rtnprd=datetime.strptime(rtnprd,"%m%Y")
        rtnprd=rtnprd.strftime("%m/%d/%Y")
    except:
        rtnprd=pd.NaT
    
    try:
        fileIndex=str(data["fileIndex"])
    except:
        fileIndex="no index"
    try:
        totalFiles=str(data["totalFiles"])

    except:
        totalFiles="no file number"
        
    try:
        chksum=data["r2bdata"]["chksum"]
    except:
        chksum="no checksum"

    try:
        gendt=data["r2bdata"]["data"]["gendt"]
        gendt=datetime.strptime(gendt,"%d-%m-%Y")
        gendt=gendt.strftime("%m/%d/%Y")
    except:
        gendt=pd.NaT

    try:
        data_import_date=datetime.now().date()
        data_import_date=data_import_date.strftime("%Y-%m-%d")
        
    except:
        data_import_date="No date"

    try:
        itcavl_nonrevsup_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["igst"]
    except:
        itcavl_nonrevsup_igst=0.00
    
    try:
        itcavl_nonrevsup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cgst"]

    except:
        itcavl_nonrevsup_cgst=0.00
    
    try:
        itcavl_nonrevsup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["sgst"]
    except:
        itcavl_nonrevsup_sgst=0.00

    try:
        itcavl_nonrevsup_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cess"]

    except:
        itcavl_nonrevsup_cess=0.00

    try:
        itcavl_nonrevsup_b2b_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2b"]["txval"]

    except:
        itcavl_nonrevsup_b2b_txval=0.00

    try:
        itcavl_nonrevsup_b2b_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2b"]["igst"]

    except:
        itcavl_nonrevsup_b2b_igst=0.00

    try:
        itcavl_nonrevsup_b2b_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2b"]["cgst"]

    except:
        itcavl_nonrevsup_b2b_cgst=0.00

    try:
        itcavl_nonrevsup_b2b_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2b"]["sgst"]
    except:
        itcavl_nonrevsup_b2b_sgst=0.00

    try:
        itcavl_nonrevsup_b2b_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2b"]["cess"]
    except:
        itcavl_nonrevsup_b2b_cess=0.00

    try:
        itcavl_nonrevsup_b2ba_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2ba"]["txval"]

    except:
        itcavl_nonrevsup_b2ba_txval=0.00

    try:
        itcavl_nonrevsup_b2ba_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2ba"]["igst"]

    except:
        itcavl_nonrevsup_b2ba_igst=0.00

    try:
        itcavl_nonrevsup_b2ba_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2ba"]["cgst"]

    except:
        itcavl_nonrevsup_b2ba_cgst=0.00
    try:
        itcavl_nonrevsup_b2ba_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2ba"]["sgst"]

    except:
        itcavl_nonrevsup_b2ba_sgst=0.00

    try:
        itcavl_nonrevsup_b2ba_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["b2ba"]["cess"]

    except:
        itcavl_nonrevsup_b2ba_cess=0.00

    try:
        itcavl_nonrevsup_cdnr_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnr"]["txval"]

    except:
        itcavl_nonrevsup_cdnr_txval=0.00

    try:
        itcavl_nonrevsup_cdnr_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnr"]["igst"]

    except:
        itcavl_nonrevsup_cdnr_igst=0.00

    try:
        itcavl_nonrevsup_cdnr_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnr"]["cgst"]

    except:
        itcavl_nonrevsup_cdnr_cgst=0.00

    try:
        itcavl_nonrevsup_cdnr_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnr"]["sgst"]

    except:
        itcavl_nonrevsup_cdnr_sgst=0.00

    try:
        itcavl_nonrevsup_cdnr_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnr"]["cess"]

    except:
        itcavl_nonrevsup_cdnr_cess=0.00

    try:
        itcavl_nonrevsup_cdnra_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnra"]["txval"]

    except:
        itcavl_nonrevsup_cdnra_txval=0.00

    try:
        itcavl_nonrevsup_cdnra_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnra"]["igst"]

    except:
        itcavl_nonrevsup_cdnra_igst=0.00

    try:
        itcavl_nonrevsup_cdnra_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnra"]["cgst"]

    except:
        itcavl_nonrevsup_cdnra_cgst=0.00

    try:
        itcavl_nonrevsup_cdnra_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnra"]["sgst"]

    except:
        itcavl_nonrevsup_cdnra_sgst=0.00

    try:
        itcavl_nonrevsup_cdnra_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["cdnra"]["cess"]

    except:
        itcavl_nonrevsup_cdnra_cess=0.00

    try:
        itcavl_nonrevsup_ecom_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecom"]["txval"]

    except:
        itcavl_nonrevsup_ecom_txval=0.00

    try:
        itcavl_nonrevsup_ecom_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecom"]["igst"]

    except:
        itcavl_nonrevsup_ecom_igst=0.00

    try:
        itcavl_nonrevsup_ecom_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecom"]["cgst"]

    except:
        itcavl_nonrevsup_ecom_cgst=0.00

    try:
        itcavl_nonrevsup_ecom_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecom"]["sgst"]

    except:
        itcavl_nonrevsup_ecom_sgst=0.00

    try:
        itcavl_nonrevsup_ecom_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecom"]["cess"]

    except:
        itcavl_nonrevsup_ecom_cess=0.00



    try:
        itcavl_nonrevsup_ecoma_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecoma"]["txval"]

    except:
        itcavl_nonrevsup_ecoma_txval=0.00

    try:
        itcavl_nonrevsup_ecoma_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecoma"]["igst"]

    except:
        itcavl_nonrevsup_ecoma_igst=0.00

    try:
        itcavl_nonrevsup_ecoma_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecoma"]["cgst"]

    except:
        itcavl_nonrevsup_ecoma_cgst=0.00

    try:
        itcavl_nonrevsup_ecoma_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecoma"]["sgst"]

    except:
        itcavl_nonrevsup_ecoma_sgst=0.00

    try:
        itcavl_nonrevsup_ecoma_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["nonrevsup"]["ecoma"]["cess"]

    except:
        itcavl_nonrevsup_ecoma_cess=0.00







    try:
        itcavl_revsup_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["igst"]
    except:
        itcavl_revsup_igst=0.00
    
    try:
        itcavl_revsup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cgst"]

    except:
        itcavl_revsup_cgst=0.00
    
    try:
        itcavl_revsup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["sgst"]
    except:
        itcavl_revsup_sgst=0.00

    try:
        itcavl_revsup_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cess"]

    except:
        itcavl_revsup_cess=0.00

    try:
        itcavl_revsup_b2b_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2b"]["txval"]

    except:
        itcavl_revsup_b2b_txval=0.00

    try:
        itcavl_revsup_b2b_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2b"]["igst"]

    except:
        itcavl_revsup_b2b_igst=0.00

    try:
        itcavl_revsup_b2b_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2b"]["cgst"]

    except:
        itcavl_revsup_b2b_cgst=0.00

    try:
        itcavl_revsup_b2b_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2b"]["sgst"]
    except:
        itcavl_revsup_b2b_sgst=0.00

    try:
        itcavl_revsup_b2b_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2b"]["cess"]
    except:
        itcavl_revsup_b2b_cess=0.00

    try:
        itcavl_revsup_b2ba_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2ba"]["txval"]

    except:
        itcavl_revsup_b2ba_txval=0.00

    try:
        itcavl_revsup_b2ba_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2ba"]["igst"]

    except:
        itcavl_revsup_b2ba_igst=0.00

    try:
        itcavl_revsup_b2ba_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2ba"]["cgst"]

    except:
        itcavl_revsup_b2ba_cgst=0.00
    try:
        itcavl_revsup_b2ba_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2ba"]["sgst"]

    except:
        itcavl_revsup_b2ba_sgst=0.00

    try:
        itcavl_revsup_b2ba_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["b2ba"]["cess"]

    except:
        itcavl_revsup_b2ba_cess=0.00

    try:
        itcavl_revsup_cdnr_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnr"]["txval"]

    except:
        itcavl_revsup_cdnr_txval=0.00

    try:
        itcavl_revsup_cdnr_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnr"]["igst"]

    except:
        itcavl_revsup_cdnr_igst=0.00

    try:
        itcavl_revsup_cdnr_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnr"]["cgst"]

    except:
        itcavl_revsup_cdnr_cgst=0.00

    try:
        itcavl_revsup_cdnr_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnr"]["sgst"]

    except:
        itcavl_revsup_cdnr_sgst=0.00

    try:
        itcavl_revsup_cdnr_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnr"]["cess"]

    except:
        itcavl_revsup_cdnr_cess=0.00

    try:
        itcavl_revsup_cdnra_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnra"]["txval"]

    except:
        itcavl_revsup_cdnra_txval=0.00

    try:
        itcavl_revsup_cdnra_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnra"]["igst"]

    except:
        itcavl_revsup_cdnra_igst=0.00

    try:
        itcavl_revsup_cdnra_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnra"]["cgst"]

    except:
        itcavl_revsup_cdnra_cgst=0.00

    try:
        itcavl_revsup_cdnra_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnra"]["sgst"]

    except:
        itcavl_revsup_cdnra_sgst=0.00

    try:
        itcavl_revsup_cdnra_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["revsup"]["cdnra"]["cess"]

    except:
        itcavl_revsup_cdnra_cess=0.00

    



    try:
        itcavl_isdsup_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["igst"]
    except:
        itcavl_isdsup_igst=0.00
    
    try:
        itcavl_isdsup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["cgst"]

    except:
        itcavl_isdsup_cgst=0.00
    
    try:
        itcavl_isdsup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["sgst"]
    except:
        itcavl_isdsup_sgst=0.00

    try:
        itcavl_isdsup_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["cess"]

    except:
        itcavl_isdsup_cess=0.00

    

    try:
        itcavl_isdsup_isd_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isd"]["txval"]

    except:
        itcavl_isdsup_isd_txval=0.00

    try:
        itcavl_isdsup_isd_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isd"]["igst"]

    except:
        itcavl_isdsup_isd_igst=0.00

    try:
        itcavl_isdsup_isd_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isd"]["cgst"]

    except:
        itcavl_isdsup_isd_cgst=0.00

    try:
        itcavl_isdsup_isd_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isd"]["sgst"]

    except:
        itcavl_isdsup_isd_sgst=0.00

    try:
        itcavl_isdsup_isd_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isd"]["cess"]

    except:
        itcavl_isdsup_isd_cess=0.00



    try:
        itcavl_isdsup_isda_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isda"]["txval"]

    except:
        itcavl_isdsup_isda_txval=0.00

    try:
        itcavl_isdsup_isda_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isda"]["igst"]

    except:
        itcavl_isdsup_isda_igst=0.00

    try:
        itcavl_isdsup_isda_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isda"]["cgst"]

    except:
        itcavl_isdsup_isda_cgst=0.00

    try:
        itcavl_isdsup_isda_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isda"]["sgst"]

    except:
        itcavl_isdsup_isda_sgst=0.00

    try:
        itcavl_isdsup_isda_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["isdsup"]["isda"]["cess"]

    except:
        itcavl_isdsup_isda_cess=0.00








    try:
        itcavl_othersup_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["igst"]
    except:
        itcavl_othersup_igst=0.00
    
    try:
        itcavl_othersup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cgst"]

    except:
        itcavl_othersup_cgst=0.00
    
    try:
        itcavl_othersup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["sgst"]
    except:
        itcavl_othersup_sgst=0.00

    try:
        itcavl_othersup_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cess"]

    except:
        itcavl_othersup_cess=0.00

    try:
        itcavl_othersup_cdnrrev_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrrev"]["txval"]

    except:
        itcavl_othersup_cdnrrev_txval=0.00

    try:
        itcavl_othersup_cdnrrev_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrrev"]["igst"]

    except:
        itcavl_othersup_cdnrrev_igst=0.00

    try:
        itcavl_othersup_cdnrrev_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrrev"]["cgst"]

    except:
        itcavl_othersup_cdnrrev_cgst=0.00

    try:
        itcavl_othersup_cdnrrev_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrrev"]["sgst"]
    except:
        itcavl_othersup_cdnrrev_sgst=0.00

    try:
        itcavl_othersup_cdnrrev_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrrev"]["cess"]
    except:
        itcavl_othersup_cdnrrev_cess=0.00

    try:
        itcavl_othersup_cdnrreva_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrreva"]["txval"]

    except:
        itcavl_othersup_cdnrreva_txval=0.00

    try:
        itcavl_othersup_cdnrreva_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrreva"]["igst"]

    except:
        itcavl_othersup_cdnrreva_igst=0.00

    try:
        itcavl_othersup_cdnrreva_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrreva"]["cgst"]

    except:
        itcavl_othersup_cdnrreva_cgst=0.00
    try:
        itcavl_othersup_cdnrreva_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrreva"]["sgst"]

    except:
        itcavl_othersup_cdnrreva_sgst=0.00

    try:
        itcavl_othersup_cdnrreva_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnrreva"]["cess"]

    except:
        itcavl_othersup_cdnrreva_cess=0.00

    try:
        itcavl_othersup_cdnr_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnr"]["txval"]

    except:
        itcavl_othersup_cdnr_txval=0.00

    try:
        itcavl_othersup_cdnr_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnr"]["igst"]

    except:
        itcavl_othersup_cdnr_igst=0.00

    try:
        itcavl_othersup_cdnr_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnr"]["cgst"]

    except:
        itcavl_othersup_cdnr_cgst=0.00

    try:
        itcavl_othersup_cdnr_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnr"]["sgst"]

    except:
        itcavl_othersup_cdnr_sgst=0.00

    try:
        itcavl_othersup_cdnr_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnr"]["cess"]

    except:
        itcavl_othersup_cdnr_cess=0.00

    try:
        itcavl_othersup_cdnra_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnra"]["txval"]

    except:
        itcavl_othersup_cdnra_txval=0.00

    try:
        itcavl_othersup_cdnra_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnra"]["igst"]

    except:
        itcavl_othersup_cdnra_igst=0.00

    try:
        itcavl_othersup_cdnra_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnra"]["cgst"]

    except:
        itcavl_othersup_cdnra_cgst=0.00

    try:
        itcavl_othersup_cdnra_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnra"]["sgst"]

    except:
        itcavl_othersup_cdnra_sgst=0.00

    try:
        itcavl_othersup_cdnra_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["cdnra"]["cess"]

    except:
        itcavl_othersup_cdnra_cess=0.00

    try:
        itcavl_othersup_isd_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isd"]["txval"]

    except:
        itcavl_othersup_isd_txval=0.00

    try:
        itcavl_othersup_isd_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isd"]["igst"]

    except:
        itcavl_othersup_isd_igst=0.00

    try:
        itcavl_othersup_isd_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isd"]["cgst"]

    except:
        itcavl_othersup_isd_cgst=0.00

    try:
        itcavl_othersup_isd_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isd"]["sgst"]

    except:
        itcavl_othersup_isd_sgst=0.00

    try:
        itcavl_othersup_isd_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isd"]["cess"]

    except:
        itcavl_othersup_isd_cess=0.00



    try:
        itcavl_othersup_isda_txval=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isda"]["txval"]

    except:
        itcavl_othersup_isda_txval=0.00

    try:
        itcavl_othersup_isda_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isda"]["igst"]

    except:
        itcavl_othersup_isda_igst=0.00

    try:
        itcavl_othersup_isda_cgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isda"]["cgst"]

    except:
        itcavl_othersup_isda_cgst=0.00

    try:
        itcavl_othersup_isda_sgst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isda"]["sgst"]

    except:
        itcavl_othersup_isda_sgst=0.00

    try:
        itcavl_othersup_isda_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["othersup"]["isda"]["cess"]

    except:
        itcavl_othersup_isda_cess=0.00




    try:
        itcavl_imports_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["igst"]

    except:
        itcavl_imports_igst=0.00
    

    try:
        itcavl_imports_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["cess"]

    except:
        itcavl_imports_cess=0.00

    
    try:
        itcavl_imports_impg_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impg"]["igst"]

    except:
        itcavl_imports_impg_igst=0.00
    

    try:
        itcavl_imports_impg_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impg"]["cess"]

    except:
        itcavl_imports_impg_cess=0.00
    

    try:
        itcavl_imports_impga_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impga"]["igst"]

    except:
        itcavl_imports_impga_igst=0.00
    

    try:
        itcavl_imports_impga_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impga"]["cess"]

    except:
        itcavl_imports_impga_cess=0.00
    

    
    try:
        itcavl_imports_impgsez_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impgsez"]["igst"]

    except:
        itcavl_imports_impgsez_igst=0.00
    

    try:
        itcavl_imports_impgsez_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impgsez"]["cess"]

    except:
        itcavl_imports_impgsez_cess=0.00

    try:
        itcavl_imports_impgseza_igst=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impgseza"]["igst"]

    except:
        itcavl_imports_impgseza_igst=0.00
    

    try:
        itcavl_imports_impgseza_cess=data["r2bdata"]["data"]["itcsumm"]["itcavl"]["imports"]["impgseza"]["cess"]

    except:
        itcavl_imports_impgseza_cess=0.00




    












    try:
        itcunavl_nonrevsup_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["igst"]
    except:
        itcunavl_nonrevsup_igst=0.00
    
    try:
        itcunavl_nonrevsup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cgst"]

    except:
        itcunavl_nonrevsup_cgst=0.00
    
    try:
        itcunavl_nonrevsup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["sgst"]
    except:
        itcunavl_nonrevsup_sgst=0.00

    try:
        itcunavl_nonrevsup_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cess"]

    except:
        itcunavl_nonrevsup_cess=0.00

    try:
        itcunavl_nonrevsup_b2b_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2b"]["txval"]

    except:
        itcunavl_nonrevsup_b2b_txval=0.00

    try:
        itcunavl_nonrevsup_b2b_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2b"]["igst"]

    except:
        itcunavl_nonrevsup_b2b_igst=0.00

    try:
        itcunavl_nonrevsup_b2b_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2b"]["cgst"]

    except:
        itcunavl_nonrevsup_b2b_cgst=0.00

    try:
        itcunavl_nonrevsup_b2b_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2b"]["sgst"]
    except:
        itcunavl_nonrevsup_b2b_sgst=0.00

    try:
        itcunavl_nonrevsup_b2b_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2b"]["cess"]
    except:
        itcunavl_nonrevsup_b2b_cess=0.00

    try:
        itcunavl_nonrevsup_b2ba_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2ba"]["txval"]

    except:
        itcunavl_nonrevsup_b2ba_txval=0.00

    try:
        itcunavl_nonrevsup_b2ba_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2ba"]["igst"]

    except:
        itcunavl_nonrevsup_b2ba_igst=0.00

    try:
        itcunavl_nonrevsup_b2ba_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2ba"]["cgst"]

    except:
        itcunavl_nonrevsup_b2ba_cgst=0.00
    try:
        itcunavl_nonrevsup_b2ba_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2ba"]["sgst"]

    except:
        itcunavl_nonrevsup_b2ba_sgst=0.00

    try:
        itcunavl_nonrevsup_b2ba_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["b2ba"]["cess"]

    except:
        itcunavl_nonrevsup_b2ba_cess=0.00

    try:
        itcunavl_nonrevsup_cdnr_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnr"]["txval"]

    except:
        itcunavl_nonrevsup_cdnr_txval=0.00

    try:
        itcunavl_nonrevsup_cdnr_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnr"]["igst"]

    except:
        itcunavl_nonrevsup_cdnr_igst=0.00

    try:
        itcunavl_nonrevsup_cdnr_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnr"]["cgst"]

    except:
        itcunavl_nonrevsup_cdnr_cgst=0.00

    try:
        itcunavl_nonrevsup_cdnr_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnr"]["sgst"]

    except:
        itcunavl_nonrevsup_cdnr_sgst=0.00

    try:
        itcunavl_nonrevsup_cdnr_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnr"]["cess"]

    except:
        itcunavl_nonrevsup_cdnr_cess=0.00

    try:
        itcunavl_nonrevsup_cdnra_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnra"]["txval"]

    except:
        itcunavl_nonrevsup_cdnra_txval=0.00

    try:
        itcunavl_nonrevsup_cdnra_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnra"]["igst"]

    except:
        itcunavl_nonrevsup_cdnra_igst=0.00

    try:
        itcunavl_nonrevsup_cdnra_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnra"]["cgst"]

    except:
        itcunavl_nonrevsup_cdnra_cgst=0.00

    try:
        itcunavl_nonrevsup_cdnra_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnra"]["sgst"]

    except:
        itcunavl_nonrevsup_cdnra_sgst=0.00

    try:
        itcunavl_nonrevsup_cdnra_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["cdnra"]["cess"]

    except:
        itcunavl_nonrevsup_cdnra_cess=0.00

    try:
        itcunavl_nonrevsup_ecom_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecom"]["txval"]

    except:
        itcunavl_nonrevsup_ecom_txval=0.00

    try:
        itcunavl_nonrevsup_ecom_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecom"]["igst"]

    except:
        itcunavl_nonrevsup_ecom_igst=0.00

    try:
        itcunavl_nonrevsup_ecom_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecom"]["cgst"]

    except:
        itcunavl_nonrevsup_ecom_cgst=0.00

    try:
        itcunavl_nonrevsup_ecom_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecom"]["sgst"]

    except:
        itcunavl_nonrevsup_ecom_sgst=0.00

    try:
        itcunavl_nonrevsup_ecom_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecom"]["cess"]

    except:
        itcunavl_nonrevsup_ecom_cess=0.00



    try:
        itcunavl_nonrevsup_ecoma_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecoma"]["txval"]

    except:
        itcunavl_nonrevsup_ecoma_txval=0.00

    try:
        itcunavl_nonrevsup_ecoma_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecoma"]["igst"]

    except:
        itcunavl_nonrevsup_ecoma_igst=0.00

    try:
        itcunavl_nonrevsup_ecoma_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecoma"]["cgst"]

    except:
        itcunavl_nonrevsup_ecoma_cgst=0.00

    try:
        itcunavl_nonrevsup_ecoma_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecoma"]["sgst"]

    except:
        itcunavl_nonrevsup_ecoma_sgst=0.00

    try:
        itcunavl_nonrevsup_ecoma_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["nonrevsup"]["ecoma"]["cess"]

    except:
        itcunavl_nonrevsup_ecoma_cess=0.00







    try:
        itcunavl_revsup_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["igst"]
    except:
        itcunavl_revsup_igst=0.00
    
    try:
        itcunavl_revsup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cgst"]

    except:
        itcunavl_revsup_cgst=0.00
    
    try:
        itcunavl_revsup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["sgst"]
    except:
        itcunavl_revsup_sgst=0.00

    try:
        itcunavl_revsup_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cess"]

    except:
        itcunavl_revsup_cess=0.00

    try:
        itcunavl_revsup_b2b_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2b"]["txval"]

    except:
        itcunavl_revsup_b2b_txval=0.00

    try:
        itcunavl_revsup_b2b_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2b"]["igst"]

    except:
        itcunavl_revsup_b2b_igst=0.00

    try:
        itcunavl_revsup_b2b_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2b"]["cgst"]

    except:
        itcunavl_revsup_b2b_cgst=0.00

    try:
        itcunavl_revsup_b2b_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2b"]["sgst"]
    except:
        itcunavl_revsup_b2b_sgst=0.00

    try:
        itcunavl_revsup_b2b_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2b"]["cess"]
    except:
        itcunavl_revsup_b2b_cess=0.00

    try:
        itcunavl_revsup_b2ba_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2ba"]["txval"]

    except:
        itcunavl_revsup_b2ba_txval=0.00

    try:
        itcunavl_revsup_b2ba_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2ba"]["igst"]

    except:
        itcunavl_revsup_b2ba_igst=0.00

    try:
        itcunavl_revsup_b2ba_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2ba"]["cgst"]

    except:
        itcunavl_revsup_b2ba_cgst=0.00
    try:
        itcunavl_revsup_b2ba_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2ba"]["sgst"]

    except:
        itcunavl_revsup_b2ba_sgst=0.00

    try:
        itcunavl_revsup_b2ba_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["b2ba"]["cess"]

    except:
        itcunavl_revsup_b2ba_cess=0.00

    try:
        itcunavl_revsup_cdnr_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnr"]["txval"]

    except:
        itcunavl_revsup_cdnr_txval=0.00

    try:
        itcunavl_revsup_cdnr_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnr"]["igst"]

    except:
        itcunavl_revsup_cdnr_igst=0.00

    try:
        itcunavl_revsup_cdnr_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnr"]["cgst"]

    except:
        itcunavl_revsup_cdnr_cgst=0.00

    try:
        itcunavl_revsup_cdnr_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnr"]["sgst"]

    except:
        itcunavl_revsup_cdnr_sgst=0.00

    try:
        itcunavl_revsup_cdnr_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnr"]["cess"]

    except:
        itcunavl_revsup_cdnr_cess=0.00

    try:
        itcunavl_revsup_cdnra_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnra"]["txval"]

    except:
        itcunavl_revsup_cdnra_txval=0.00

    try:
        itcunavl_revsup_cdnra_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnra"]["igst"]

    except:
        itcunavl_revsup_cdnra_igst=0.00

    try:
        itcunavl_revsup_cdnra_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnra"]["cgst"]

    except:
        itcunavl_revsup_cdnra_cgst=0.00

    try:
        itcunavl_revsup_cdnra_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnra"]["sgst"]

    except:
        itcunavl_revsup_cdnra_sgst=0.00

    try:
        itcunavl_revsup_cdnra_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["revsup"]["cdnra"]["cess"]

    except:
        itcunavl_revsup_cdnra_cess=0.00

    



    try:
        itcunavl_isdsup_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["igst"]
    except:
        itcunavl_isdsup_igst=0.00
    
    try:
        itcunavl_isdsup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["cgst"]

    except:
        itcunavl_isdsup_cgst=0.00
    
    try:
        itcunavl_isdsup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["sgst"]
    except:
        itcunavl_isdsup_sgst=0.00

    try:
        itcunavl_isdsup_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["cess"]

    except:
        itcunavl_isdsup_cess=0.00

    

    try:
        itcunavl_isdsup_isd_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isd"]["txval"]

    except:
        itcunavl_isdsup_isd_txval=0.00

    try:
        itcunavl_isdsup_isd_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isd"]["igst"]

    except:
        itcunavl_isdsup_isd_igst=0.00

    try:
        itcunavl_isdsup_isd_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isd"]["cgst"]

    except:
        itcunavl_isdsup_isd_cgst=0.00

    try:
        itcunavl_isdsup_isd_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isd"]["sgst"]

    except:
        itcunavl_isdsup_isd_sgst=0.00

    try:
        itcunavl_isdsup_isd_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isd"]["cess"]

    except:
        itcunavl_isdsup_isd_cess=0.00



    try:
        itcunavl_isdsup_isda_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isda"]["txval"]

    except:
        itcunavl_isdsup_isda_txval=0.00

    try:
        itcunavl_isdsup_isda_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isda"]["igst"]

    except:
        itcunavl_isdsup_isda_igst=0.00

    try:
        itcunavl_isdsup_isda_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isda"]["cgst"]

    except:
        itcunavl_isdsup_isda_cgst=0.00

    try:
        itcunavl_isdsup_isda_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isda"]["sgst"]

    except:
        itcunavl_isdsup_isda_sgst=0.00

    try:
        itcunavl_isdsup_isda_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["isdsup"]["isda"]["cess"]

    except:
        itcunavl_isdsup_isda_cess=0.00








    try:
        itcunavl_othersup_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["igst"]
    except:
        itcunavl_othersup_igst=0.00
    
    try:
        itcunavl_othersup_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cgst"]

    except:
        itcunavl_othersup_cgst=0.00
    
    try:
        itcunavl_othersup_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["sgst"]
    except:
        itcunavl_othersup_sgst=0.00

    try:
        itcunavl_othersup_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cess"]

    except:
        itcunavl_othersup_cess=0.00

    try:
        itcunavl_othersup_cdnrrev_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrrev"]["txval"]

    except:
        itcunavl_othersup_cdnrrev_txval=0.00

    try:
        itcunavl_othersup_cdnrrev_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrrev"]["igst"]

    except:
        itcunavl_othersup_cdnrrev_igst=0.00

    try:
        itcunavl_othersup_cdnrrev_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrrev"]["cgst"]

    except:
        itcunavl_othersup_cdnrrev_cgst=0.00

    try:
        itcunavl_othersup_cdnrrev_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrrev"]["sgst"]
    except:
        itcunavl_othersup_cdnrrev_sgst=0.00

    try:
        itcunavl_othersup_cdnrrev_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrrev"]["cess"]
    except:
        itcunavl_othersup_cdnrrev_cess=0.00

    try:
        itcunavl_othersup_cdnrreva_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrreva"]["txval"]

    except:
        itcunavl_othersup_cdnrreva_txval=0.00

    try:
        itcunavl_othersup_cdnrreva_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrreva"]["igst"]

    except:
        itcunavl_othersup_cdnrreva_igst=0.00

    try:
        itcunavl_othersup_cdnrreva_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrreva"]["cgst"]

    except:
        itcunavl_othersup_cdnrreva_cgst=0.00
    try:
        itcunavl_othersup_cdnrreva_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrreva"]["sgst"]

    except:
        itcunavl_othersup_cdnrreva_sgst=0.00

    try:
        itcunavl_othersup_cdnrreva_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnrreva"]["cess"]

    except:
        itcunavl_othersup_cdnrreva_cess=0.00

    try:
        itcunavl_othersup_cdnr_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnr"]["txval"]

    except:
        itcunavl_othersup_cdnr_txval=0.00

    try:
        itcunavl_othersup_cdnr_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnr"]["igst"]

    except:
        itcunavl_othersup_cdnr_igst=0.00

    try:
        itcunavl_othersup_cdnr_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnr"]["cgst"]

    except:
        itcunavl_othersup_cdnr_cgst=0.00

    try:
        itcunavl_othersup_cdnr_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnr"]["sgst"]

    except:
        itcunavl_othersup_cdnr_sgst=0.00

    try:
        itcunavl_othersup_cdnr_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnr"]["cess"]

    except:
        itcunavl_othersup_cdnr_cess=0.00

    try:
        itcunavl_othersup_cdnra_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnra"]["txval"]

    except:
        itcunavl_othersup_cdnra_txval=0.00

    try:
        itcunavl_othersup_cdnra_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnra"]["igst"]

    except:
        itcunavl_othersup_cdnra_igst=0.00

    try:
        itcunavl_othersup_cdnra_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnra"]["cgst"]

    except:
        itcunavl_othersup_cdnra_cgst=0.00

    try:
        itcunavl_othersup_cdnra_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnra"]["sgst"]

    except:
        itcunavl_othersup_cdnra_sgst=0.00

    try:
        itcunavl_othersup_cdnra_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["cdnra"]["cess"]

    except:
        itcunavl_othersup_cdnra_cess=0.00

    try:
        itcunavl_othersup_isd_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isd"]["txval"]

    except:
        itcunavl_othersup_isd_txval=0.00

    try:
        itcunavl_othersup_isd_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isd"]["igst"]

    except:
        itcunavl_othersup_isd_igst=0.00

    try:
        itcunavl_othersup_isd_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isd"]["cgst"]

    except:
        itcunavl_othersup_isd_cgst=0.00

    try:
        itcunavl_othersup_isd_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isd"]["sgst"]

    except:
        itcunavl_othersup_isd_sgst=0.00

    try:
        itcunavl_othersup_isd_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isd"]["cess"]

    except:
        itcunavl_othersup_isd_cess=0.00



    try:
        itcunavl_othersup_isda_txval=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isda"]["txval"]

    except:
        itcunavl_othersup_isda_txval=0.00

    try:
        itcunavl_othersup_isda_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isda"]["igst"]

    except:
        itcunavl_othersup_isda_igst=0.00

    try:
        itcunavl_othersup_isda_cgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isda"]["cgst"]

    except:
        itcunavl_othersup_isda_cgst=0.00

    try:
        itcunavl_othersup_isda_sgst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isda"]["sgst"]

    except:
        itcunavl_othersup_isda_sgst=0.00

    try:
        itcunavl_othersup_isda_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["othersup"]["isda"]["cess"]

    except:
        itcunavl_othersup_isda_cess=0.00




    try:
        itcunavl_imports_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["igst"]

    except:
        itcunavl_imports_igst=0.00
    

    try:
        itcunavl_imports_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["cess"]

    except:
        itcunavl_imports_cess=0.00

    
    try:
        itcunavl_imports_impg_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impg"]["igst"]

    except:
        itcunavl_imports_impg_igst=0.00
    

    try:
        itcunavl_imports_impg_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impg"]["cess"]

    except:
        itcunavl_imports_impg_cess=0.00
    

    try:
        itcunavl_imports_impga_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impga"]["igst"]

    except:
        itcunavl_imports_impga_igst=0.00
    

    try:
        itcunavl_imports_impga_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impga"]["cess"]

    except:
        itcunavl_imports_impga_cess=0.00
    

    
    try:
        itcunavl_imports_impgsez_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impgsez"]["igst"]

    except:
        itcunavl_imports_impgsez_igst=0.00
    

    try:
        itcunavl_imports_impgsez_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impgsez"]["cess"]

    except:
        itcunavl_imports_impgsez_cess=0.00

    try:
        itcunavl_imports_impgseza_igst=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impgseza"]["igst"]

    except:
        itcunavl_imports_impgseza_igst=0.00
    

    try:
        itcunavl_imports_impgseza_cess=data["r2bdata"]["data"]["itcsumm"]["itcunavl"]["imports"]["impgseza"]["cess"]

    except:
        itcunavl_imports_impgseza_cess=0.00




    a=[gstin,Financial_year,rtnprd,fileIndex,totalFiles,chksum,gendt,\
    itcavl_nonrevsup_igst,itcavl_nonrevsup_cgst,itcavl_nonrevsup_sgst,itcavl_nonrevsup_cess,\
    itcavl_nonrevsup_b2b_txval,itcavl_nonrevsup_b2b_igst,itcavl_nonrevsup_b2b_cgst,itcavl_nonrevsup_b2b_sgst,itcavl_nonrevsup_b2b_cess,\
    itcavl_nonrevsup_b2ba_txval,itcavl_nonrevsup_b2ba_igst,itcavl_nonrevsup_b2ba_cgst,itcavl_nonrevsup_b2ba_sgst,itcavl_nonrevsup_b2ba_cess,\
    itcavl_nonrevsup_cdnr_txval,itcavl_nonrevsup_cdnr_igst,itcavl_nonrevsup_cdnr_cgst,itcavl_nonrevsup_cdnr_sgst,itcavl_nonrevsup_cdnr_cess,\
    itcavl_nonrevsup_cdnra_txval,itcavl_nonrevsup_cdnra_igst,itcavl_nonrevsup_cdnra_cgst,itcavl_nonrevsup_cdnra_sgst,itcavl_nonrevsup_cdnra_cess,\
    itcavl_nonrevsup_ecom_txval,itcavl_nonrevsup_ecom_igst,itcavl_nonrevsup_ecom_cgst,itcavl_nonrevsup_ecom_sgst,itcavl_nonrevsup_ecom_cess,\
    itcavl_nonrevsup_ecoma_txval,itcavl_nonrevsup_ecoma_igst,itcavl_nonrevsup_ecoma_cgst,itcavl_nonrevsup_ecoma_sgst,itcavl_nonrevsup_ecoma_cess,\
    itcavl_revsup_igst,itcavl_revsup_cgst,itcavl_revsup_sgst,itcavl_revsup_cess,\
    itcavl_revsup_b2b_txval,itcavl_revsup_b2b_igst,itcavl_revsup_b2b_cgst,itcavl_revsup_b2b_sgst,itcavl_revsup_b2b_cess,\
    itcavl_revsup_b2ba_txval,itcavl_revsup_b2ba_igst,itcavl_revsup_b2ba_cgst,itcavl_revsup_b2ba_sgst,itcavl_revsup_b2ba_cess,\
    itcavl_revsup_cdnr_txval,itcavl_revsup_cdnr_igst,itcavl_revsup_cdnr_cgst,itcavl_revsup_cdnr_sgst,itcavl_revsup_cdnr_cess,\
    itcavl_revsup_cdnra_txval,itcavl_revsup_cdnra_igst,itcavl_revsup_cdnra_cgst,itcavl_revsup_cdnra_sgst,itcavl_revsup_cdnra_cess,\
    itcavl_isdsup_igst,itcavl_isdsup_cgst,itcavl_isdsup_sgst,itcavl_isdsup_cess,\
    itcavl_isdsup_isd_txval,itcavl_isdsup_isd_igst,itcavl_isdsup_isd_cgst,itcavl_isdsup_isd_sgst,itcavl_isdsup_isd_cess,\
    itcavl_isdsup_isda_txval,itcavl_isdsup_isda_igst,itcavl_isdsup_isda_cgst,itcavl_isdsup_isda_sgst,itcavl_isdsup_isda_cess,\
    itcavl_othersup_igst,itcavl_othersup_cgst,itcavl_othersup_sgst,itcavl_othersup_cess,\
    itcavl_othersup_cdnrrev_txval,itcavl_othersup_cdnrrev_igst,itcavl_othersup_cdnrrev_cgst,itcavl_othersup_cdnrrev_sgst,itcavl_othersup_cdnrrev_cess,\
    itcavl_othersup_cdnrreva_txval,itcavl_othersup_cdnrreva_igst,itcavl_othersup_cdnrreva_cgst,itcavl_othersup_cdnrreva_sgst,itcavl_othersup_cdnrreva_cess,\
    itcavl_othersup_cdnr_txval,itcavl_othersup_cdnr_igst,itcavl_othersup_cdnr_cgst,itcavl_othersup_cdnr_sgst,itcavl_othersup_cdnr_cess,\
    itcavl_othersup_cdnra_txval,itcavl_othersup_cdnra_igst,itcavl_othersup_cdnra_cgst,itcavl_othersup_cdnra_sgst,itcavl_othersup_cdnra_cess,\
    itcavl_othersup_isd_txval,itcavl_othersup_isd_igst,itcavl_othersup_isd_cgst,itcavl_othersup_isd_sgst,itcavl_othersup_isd_cess,\
    itcavl_othersup_isda_txval,itcavl_othersup_isda_igst,itcavl_othersup_isda_cgst,itcavl_othersup_isda_sgst,itcavl_othersup_isda_cess,\
    itcavl_imports_igst,itcavl_imports_cess,\
    itcavl_imports_impg_igst,itcavl_imports_impg_cess,\
    itcavl_imports_impga_igst,itcavl_imports_impga_cess,\
    itcavl_imports_impgsez_igst,itcavl_imports_impgsez_cess,\
    itcavl_imports_impgseza_igst,itcavl_imports_impgseza_cess,\
    itcunavl_nonrevsup_igst,itcunavl_nonrevsup_cgst,itcunavl_nonrevsup_sgst,itcunavl_nonrevsup_cess,\
    itcunavl_nonrevsup_b2b_txval,itcunavl_nonrevsup_b2b_igst,itcunavl_nonrevsup_b2b_cgst,itcunavl_nonrevsup_b2b_sgst,itcunavl_nonrevsup_b2b_cess,\
    itcunavl_nonrevsup_b2ba_txval,itcunavl_nonrevsup_b2ba_igst,itcunavl_nonrevsup_b2ba_cgst,itcunavl_nonrevsup_b2ba_sgst,itcunavl_nonrevsup_b2ba_cess,\
    itcunavl_nonrevsup_cdnr_txval,itcunavl_nonrevsup_cdnr_igst,itcunavl_nonrevsup_cdnr_cgst,itcunavl_nonrevsup_cdnr_sgst,itcunavl_nonrevsup_cdnr_cess,\
    itcunavl_nonrevsup_cdnra_txval,itcunavl_nonrevsup_cdnra_igst,itcunavl_nonrevsup_cdnra_cgst,itcunavl_nonrevsup_cdnra_sgst,itcunavl_nonrevsup_cdnra_cess,\
    itcunavl_nonrevsup_ecom_txval,itcunavl_nonrevsup_ecom_igst,itcunavl_nonrevsup_ecom_cgst,itcunavl_nonrevsup_ecom_sgst,itcunavl_nonrevsup_ecom_cess,\
    itcunavl_nonrevsup_ecoma_txval,itcunavl_nonrevsup_ecoma_igst,itcunavl_nonrevsup_ecoma_cgst,itcunavl_nonrevsup_ecoma_sgst,itcunavl_nonrevsup_ecoma_cess,\
    itcunavl_revsup_igst,itcunavl_revsup_cgst,itcunavl_revsup_sgst,itcunavl_revsup_cess,\
    itcunavl_revsup_b2b_txval,itcunavl_revsup_b2b_igst,itcunavl_revsup_b2b_cgst,itcunavl_revsup_b2b_sgst,itcunavl_revsup_b2b_cess,\
    itcunavl_revsup_b2ba_txval,itcunavl_revsup_b2ba_igst,itcunavl_revsup_b2ba_cgst,itcunavl_revsup_b2ba_sgst,itcunavl_revsup_b2ba_cess,\
    itcunavl_revsup_cdnr_txval,itcunavl_revsup_cdnr_igst,itcunavl_revsup_cdnr_cgst,itcunavl_revsup_cdnr_sgst,itcunavl_revsup_cdnr_cess,\
    itcunavl_revsup_cdnra_txval,itcunavl_revsup_cdnra_igst,itcunavl_revsup_cdnra_cgst,itcunavl_revsup_cdnra_sgst,itcunavl_revsup_cdnra_cess,\
    itcunavl_isdsup_igst,itcunavl_isdsup_cgst,itcunavl_isdsup_sgst,itcunavl_isdsup_cess,\
    itcunavl_isdsup_isd_txval,itcunavl_isdsup_isd_igst,itcunavl_isdsup_isd_cgst,itcunavl_isdsup_isd_sgst,itcunavl_isdsup_isd_cess,\
    itcunavl_isdsup_isda_txval,itcunavl_isdsup_isda_igst,itcunavl_isdsup_isda_cgst,itcunavl_isdsup_isda_sgst,itcunavl_isdsup_isda_cess,\
    itcunavl_othersup_igst,itcunavl_othersup_cgst,itcunavl_othersup_sgst,itcunavl_othersup_cess,\
    itcunavl_othersup_cdnrrev_txval,itcunavl_othersup_cdnrrev_igst,itcunavl_othersup_cdnrrev_cgst,itcunavl_othersup_cdnrrev_sgst,itcunavl_othersup_cdnrrev_cess,\
    itcunavl_othersup_cdnrreva_txval,itcunavl_othersup_cdnrreva_igst,itcunavl_othersup_cdnrreva_cgst,itcunavl_othersup_cdnrreva_sgst,itcunavl_othersup_cdnrreva_cess,\
    itcunavl_othersup_cdnr_txval,itcunavl_othersup_cdnr_igst,itcunavl_othersup_cdnr_cgst,itcunavl_othersup_cdnr_sgst,itcunavl_othersup_cdnr_cess,\
    itcunavl_othersup_cdnra_txval,itcunavl_othersup_cdnra_igst,itcunavl_othersup_cdnra_cgst,itcunavl_othersup_cdnra_sgst,itcunavl_othersup_cdnra_cess,\
    itcunavl_othersup_isd_txval,itcunavl_othersup_isd_igst,itcunavl_othersup_isd_cgst,itcunavl_othersup_isd_sgst,itcunavl_othersup_isd_cess,\
    itcunavl_othersup_isda_txval,itcunavl_othersup_isda_igst,itcunavl_othersup_isda_cgst,itcunavl_othersup_isda_sgst,itcunavl_othersup_isda_cess,\
    itcunavl_imports_igst,itcunavl_imports_cess,\
    itcunavl_imports_impg_igst,itcunavl_imports_impg_cess,\
    itcunavl_imports_impga_igst,itcunavl_imports_impga_cess,\
    itcunavl_imports_impgsez_igst,itcunavl_imports_impgsez_cess,\
    itcunavl_imports_impgseza_igst,itcunavl_imports_impgseza_cess,data_import_date]

    b=["gstin","Financial_year","rtnprd","fileIndex","totalFiles","chksum","gendt",\
    "itcavl_nonrevsup_igst","itcavl_nonrevsup_cgst","itcavl_nonrevsup_sgst","itcavl_nonrevsup_cess",\
    "itcavl_nonrevsup_b2b_txval","itcavl_nonrevsup_b2b_igst","itcavl_nonrevsup_b2b_cgst","itcavl_nonrevsup_b2b_sgst","itcavl_nonrevsup_b2b_cess",\
    "itcavl_nonrevsup_b2ba_txval","itcavl_nonrevsup_b2ba_igst","itcavl_nonrevsup_b2ba_cgst","itcavl_nonrevsup_b2ba_sgst","itcavl_nonrevsup_b2ba_cess",\
    "itcavl_nonrevsup_cdnr_txval","itcavl_nonrevsup_cdnr_igst","itcavl_nonrevsup_cdnr_cgst","itcavl_nonrevsup_cdnr_sgst","itcavl_nonrevsup_cdnr_cess",\
    "itcavl_nonrevsup_cdnra_txval","itcavl_nonrevsup_cdnra_igst","itcavl_nonrevsup_cdnra_cgst","itcavl_nonrevsup_cdnra_sgst","itcavl_nonrevsup_cdnra_cess",\
    "itcavl_nonrevsup_ecom_txval","itcavl_nonrevsup_ecom_igst","itcavl_nonrevsup_ecom_cgst","itcavl_nonrevsup_ecom_sgst","itcavl_nonrevsup_ecom_cess",\
    "itcavl_nonrevsup_ecoma_txval","itcavl_nonrevsup_ecoma_igst","itcavl_nonrevsup_ecoma_cgst","itcavl_nonrevsup_ecoma_sgst","itcavl_nonrevsup_ecoma_cess",\
    "itcavl_revsup_igst","itcavl_revsup_cgst","itcavl_revsup_sgst","itcavl_revsup_cess",\
    "itcavl_revsup_b2b_txval","itcavl_revsup_b2b_igst","itcavl_revsup_b2b_cgst","itcavl_revsup_b2b_sgst","itcavl_revsup_b2b_cess",\
    "itcavl_revsup_b2ba_txval","itcavl_revsup_b2ba_igst","itcavl_revsup_b2ba_cgst","itcavl_revsup_b2ba_sgst","itcavl_revsup_b2ba_cess",\
    "itcavl_revsup_cdnr_txval","itcavl_revsup_cdnr_igst","itcavl_revsup_cdnr_cgst","itcavl_revsup_cdnr_sgst","itcavl_revsup_cdnr_cess",\
    "itcavl_revsup_cdnra_txval","itcavl_revsup_cdnra_igst","itcavl_revsup_cdnra_cgst","itcavl_revsup_cdnra_sgst","itcavl_revsup_cdnra_cess",\
    "itcavl_isdsup_igst","itcavl_isdsup_cgst","itcavl_isdsup_sgst","itcavl_isdsup_cess",\
    "itcavl_isdsup_isd_txval","itcavl_isdsup_isd_igst","itcavl_isdsup_isd_cgst","itcavl_isdsup_isd_sgst","itcavl_isdsup_isd_cess",\
    "itcavl_isdsup_isda_txval","itcavl_isdsup_isda_igst","itcavl_isdsup_isda_cgst","itcavl_isdsup_isda_sgst","itcavl_isdsup_isda_cess",\
    "itcavl_othersup_igst","itcavl_othersup_cgst","itcavl_othersup_sgst","itcavl_othersup_cess",\
    "itcavl_othersup_cdnrrev_txval","itcavl_othersup_cdnrrev_igst","itcavl_othersup_cdnrrev_cgst","itcavl_othersup_cdnrrev_sgst","itcavl_othersup_cdnrrev_cess",\
    "itcavl_othersup_cdnrreva_txval","itcavl_othersup_cdnrreva_igst","itcavl_othersup_cdnrreva_cgst","itcavl_othersup_cdnrreva_sgst","itcavl_othersup_cdnrreva_cess",\
    "itcavl_othersup_cdnr_txval","itcavl_othersup_cdnr_igst","itcavl_othersup_cdnr_cgst","itcavl_othersup_cdnr_sgst","itcavl_othersup_cdnr_cess",\
    "itcavl_othersup_cdnra_txval","itcavl_othersup_cdnra_igst","itcavl_othersup_cdnra_cgst","itcavl_othersup_cdnra_sgst","itcavl_othersup_cdnra_cess",\
    "itcavl_othersup_isd_txval","itcavl_othersup_isd_igst","itcavl_othersup_isd_cgst","itcavl_othersup_isd_sgst","itcavl_othersup_isd_cess",\
    "itcavl_othersup_isda_txval","itcavl_othersup_isda_igst","itcavl_othersup_isda_cgst","itcavl_othersup_isda_sgst","itcavl_othersup_isda_cess",\
    "itcavl_imports_igst","itcavl_imports_cess",\
    "itcavl_imports_impg_igst","itcavl_imports_impg_cess",\
    "itcavl_imports_impga_igst","itcavl_imports_impga_cess",\
    "itcavl_imports_impgsez_igst","itcavl_imports_impgsez_cess",\
    "itcavl_imports_impgseza_igst","itcavl_imports_impgseza_cess",\
    "itcunavl_nonrevsup_igst","itcunavl_nonrevsup_cgst","itcunavl_nonrevsup_sgst","itcunavl_nonrevsup_cess",\
    "itcunavl_nonrevsup_b2b_txval","itcunavl_nonrevsup_b2b_igst","itcunavl_nonrevsup_b2b_cgst","itcunavl_nonrevsup_b2b_sgst","itcunavl_nonrevsup_b2b_cess",\
    "itcunavl_nonrevsup_b2ba_txval","itcunavl_nonrevsup_b2ba_igst","itcunavl_nonrevsup_b2ba_cgst","itcunavl_nonrevsup_b2ba_sgst","itcunavl_nonrevsup_b2ba_cess",\
    "itcunavl_nonrevsup_cdnr_txval","itcunavl_nonrevsup_cdnr_igst","itcunavl_nonrevsup_cdnr_cgst","itcunavl_nonrevsup_cdnr_sgst","itcunavl_nonrevsup_cdnr_cess",\
    "itcunavl_nonrevsup_cdnra_txval","itcunavl_nonrevsup_cdnra_igst","itcunavl_nonrevsup_cdnra_cgst","itcunavl_nonrevsup_cdnra_sgst","itcunavl_nonrevsup_cdnra_cess",\
    "itcunavl_nonrevsup_ecom_txval","itcunavl_nonrevsup_ecom_igst","itcunavl_nonrevsup_ecom_cgst","itcunavl_nonrevsup_ecom_sgst","itcunavl_nonrevsup_ecom_cess",\
    "itcunavl_nonrevsup_ecoma_txval","itcunavl_nonrevsup_ecoma_igst","itcunavl_nonrevsup_ecoma_cgst","itcunavl_nonrevsup_ecoma_sgst","itcunavl_nonrevsup_ecoma_cess",\
    "itcunavl_revsup_igst","itcunavl_revsup_cgst","itcunavl_revsup_sgst","itcunavl_revsup_cess",\
    "itcunavl_revsup_b2b_txval","itcunavl_revsup_b2b_igst","itcunavl_revsup_b2b_cgst","itcunavl_revsup_b2b_sgst","itcunavl_revsup_b2b_cess",\
    "itcunavl_revsup_b2ba_txval","itcunavl_revsup_b2ba_igst","itcunavl_revsup_b2ba_cgst","itcunavl_revsup_b2ba_sgst","itcunavl_revsup_b2ba_cess",\
    "itcunavl_revsup_cdnr_txval","itcunavl_revsup_cdnr_igst","itcunavl_revsup_cdnr_cgst","itcunavl_revsup_cdnr_sgst","itcunavl_revsup_cdnr_cess",\
    "itcunavl_revsup_cdnra_txval","itcunavl_revsup_cdnra_igst","itcunavl_revsup_cdnra_cgst","itcunavl_revsup_cdnra_sgst","itcunavl_revsup_cdnra_cess",\
    "itcunavl_isdsup_igst","itcunavl_isdsup_cgst","itcunavl_isdsup_sgst","itcunavl_isdsup_cess",\
    "itcunavl_isdsup_isd_txval","itcunavl_isdsup_isd_igst","itcunavl_isdsup_isd_cgst","itcunavl_isdsup_isd_sgst","itcunavl_isdsup_isd_cess",\
    "itcunavl_isdsup_isda_txval","itcunavl_isdsup_isda_igst","itcunavl_isdsup_isda_cgst","itcunavl_isdsup_isda_sgst","itcunavl_isdsup_isda_cess",\
    "itcunavl_othersup_igst","itcunavl_othersup_cgst","itcunavl_othersup_sgst","itcunavl_othersup_cess",\
    "itcunavl_othersup_cdnrrev_txval","itcunavl_othersup_cdnrrev_igst","itcunavl_othersup_cdnrrev_cgst","itcunavl_othersup_cdnrrev_sgst","itcunavl_othersup_cdnrrev_cess",\
    "itcunavl_othersup_cdnrreva_txval","itcunavl_othersup_cdnrreva_igst","itcunavl_othersup_cdnrreva_cgst","itcunavl_othersup_cdnrreva_sgst","itcunavl_othersup_cdnrreva_cess",\
    "itcunavl_othersup_cdnr_txval","itcunavl_othersup_cdnr_igst","itcunavl_othersup_cdnr_cgst","itcunavl_othersup_cdnr_sgst","itcunavl_othersup_cdnr_cess",\
    "itcunavl_othersup_cdnra_txval","itcunavl_othersup_cdnra_igst","itcunavl_othersup_cdnra_cgst","itcunavl_othersup_cdnra_sgst","itcunavl_othersup_cdnra_cess",\
    "itcunavl_othersup_isd_txval","itcunavl_othersup_isd_igst","itcunavl_othersup_isd_cgst","itcunavl_othersup_isd_sgst","itcunavl_othersup_isd_cess",\
    "itcunavl_othersup_isda_txval","itcunavl_othersup_isda_igst","itcunavl_othersup_isda_cgst","itcunavl_othersup_isda_sgst","itcunavl_othersup_isda_cess",\
    "itcunavl_imports_igst","itcunavl_imports_cess",\
    "itcunavl_imports_impg_igst","itcunavl_imports_impg_cess",\
    "itcunavl_imports_impga_igst","itcunavl_imports_impga_cess",\
    "itcunavl_imports_impgsez_igst","itcunavl_imports_impgsez_cess",\
    "itcunavl_imports_impgseza_igst","itcunavl_imports_impgseza_cess","data_import_date"] 
    print(file)

    return(dict(zip(b,a)))
    


def convert_types(data):
    return [float(x) if isinstance(x, np.float64) else x  for x in data]



if __name__=="__main__":
    start=datetime(year=2023,month=4,day=1).date()
    # run up to yesterday (do not include today's in-progress data)
    data_import_date=(datetime.now().date() - timedelta(days=1))
    lt1=[]
    current=start
    # Collect every date from start up to today (inclusive)
    while current<=data_import_date:
        lt1.append(current)
        current = current + timedelta(days=1)

    lt2=[i.strftime("%d-%m-%Y") for i in lt1]

    pt=Path("G:/Daily json data/R2B")

    lt3=os.listdir(pt)
    lt=list(set(lt2)-set(lt3))
    
    if len(lt)!=0:
        # start_date=datetime.strptime(min(lt), '%d-%m-%Y').date()
        # end_date=datetime.strptime(max(lt), '%d-%m-%Y').date()
        # parse missing date strings into date objects (avoid datetime/date mixups)
        dates = [datetime.strptime(date, '%d-%m-%Y').date() for date in lt]
        # sort and pick the earliest and latest missing dates
        dates.sort()
        start_date = dates[0]
        end_date = dates[-1]
        # keep a sorted list for deterministic iteration
        missing_dates_sorted = dates
        print(f"Missing dates to process (count={len(missing_dates_sorted)}): {missing_dates_sorted}")
    else:
        sys.exit()

    # parse CLI args (e.g. --dry-run)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--dry-run', action='store_true', help='Create folders and simulate downloads without network requests')
    args, _ = parser.parse_known_args()
    DRY_RUN = args.dry_run





    # start_date=datetime(year=2024,month=8,day=21)
    # end_date=datetime(year=2024,month=8,day=26)

    def get_token():
        response=requests.post(url1,headers=header1,json=body1,stream=True)
        token=response.json()["auth_token"]
        return token

    def renew_token(token_expiration):
        if datetime.now()>=token_expiration:
            return get_token()
        else:
            return None
    
    def remove_dir_if_not_empty(path):
        """Remove directory only if it contains files. Leave empty date folders intact."""
        try:
            if os.path.exists(path) and os.listdir(path):
                shutil.rmtree(path)
        except Exception:
            # ignore removal failures, leave folder in place
            pass
        
    #Presentable code for 3b, 7 and 8 
    #initialised the value of a 
    a=1
    b=1
    c=1
    d=1
    e=1
    f=1
    g=1
    h=1
    l=1







    #infinite while loop for the exception of not given reponse in the authenticate api, which will break after all the file downloaded from the start till end date.
    while True:

        #initiaslized the current date
        current_date=start_date

        try:

            #get the auth_token from the first POST API
            token=get_token()
            #again set the value of a to 1
            a=1

            # Determine the expiry time of the token
            token_expiration=datetime.now()+timedelta(minutes=110)

            #while loop which iterate from start at start_date and ends at end_date
            while current_date<=end_date:

                # Always prepare the date folder (create empty folder when needed)
                formatted_date=current_date.strftime("%d-%m-%Y")
                pt1=Path(f"{pt}/{formatted_date}")
                os.makedirs(pt1, exist_ok=True)

                # Only proceed with download flow if this date is pending
                if formatted_date in lt:

                    # If dry-run requested, just prepare the folder and skip network operations
                    if 'DRY_RUN' in globals() and DRY_RUN:
                        print(f"[DRY RUN] would process date {formatted_date} -> ensured folder {pt1}")
                        # move to next date
                        current_date += timedelta(days=1)
                        continue

                    try:
                        
                        
                        # renew the token if expire time exceed expire time from its previous generation.
                        token1=renew_token(token_expiration)
                        if token1:
                            token_expiration=datetime.now()+timedelta(minutes=110)
                            token=token1

                        #url of 1st get api for file count id appended with date
                        url_new=url2+formatted_date

                        #creating or updating the new key auth_token in header2 dictionary
                        header2["auth-token"]=token
                        b=1

                        try:
                            
                            #caliing 1st get api for generating the dictionary containing 'data' key for file count
                            response1=requests.get(url_new,headers=header2,stream=True)
                            c=1
                            
                            try:
                                
                                if "data" in list(response1.json().keys()):
                                #Extracting the 'data' key's value from the generated dictionary
                                    encoding_string=response1.json()["data"]
                                    d=1

                                elif response1.json()["error"]["message"]=="No Details Found for the Provided Inputs":
                                    print("error in response1",response1.json()["error"])
                                    d=1
                                    current_date+=timedelta(days=1)
                                    continue
                                else:
                                    if d<=20:
                                    #This date contain no .tar.gz file
                                        print("error in response1",response1.json()["error"])
                                        d=d+1
                                        continue
                                    else:
                                        remove_dir_if_not_empty(pt1)
                                        current_date+=timedelta(days=1)
                                        d=1
                                        continue
                                    
                                    

                                try:
                                    
                                    #base64 library to extract the file count from the 'data' key's value 
                                    decode_bytes=base64.b64decode(encoding_string)
                                    decoded_string=decode_bytes.decode('utf-8')
                                    #file_number contain file count in that date
                                    file_number=json.loads(decoded_string)["num_files"]
                                    #i is initialized to 1
                                    i=1
                                    e=1
                                    #while iterates through number of times equal to file count. 
                                    while i <=int(file_number):
                                        try:
                                            
                                            #token is renewd if it expires
                                            token1=renew_token(token_expiration)
                                            
                                            if token1:
                                                token_expiration=datetime.now() + timedelta(minutes=110)
                                                token=token1
                                            f=1
                                            #for file number less than equal to 9 because in 'url_new1' 'file_num' start with 0
                                            if i <10:
                                                #url for 2nd get api
                                                url_new1=f"https://boapi.internal.gst.gov.in/govtapi/v2.0/common?action=FILEDET&type=R2B&state_cd=01&date={formatted_date}&file_num=0{i}"

                                                #auth-token variable is updated in header2 dictionary
                                                header2["auth-token"]=token
                                                try:
                                                    
                                                    #Calling 2nd get API 
                                                    response2=requests.get(url_new1,headers=header2,stream=True)
                                                    # 'data' key's value is store in 'encoding_string1'
                                                    encoding_string1=response2.json()["data"]
                                                    g=1
                                                    try:
                                                        
                                                        #Extracting the downloading url from the 'encoding_string1' variable value using base64 library
                                                        decode_bytes1=base64.b64decode(encoding_string1)
                                                        decoded_string1=decode_bytes1.decode('utf-8')
                                                        #url store here
                                                        download_url=json.loads(decoded_string1)["url"]
                                                        h=1
                                                        try: 
                                                            
                                                            #download the .tar.gz file from the url and save it in the given folder 
                                                            # wget.download(download_url,out=os.path.join(f"{pt1}",f"{formatted_date}_{i}"+".tar.gz"))
                                                            response=requests.get(download_url,stream=True)
                                                            fileobj=io.BytesIO(response.content)
                                                            with tarfile.open(fileobj=fileobj,mode="r:gz") as tar:
                                                                tar.extractall(f"{pt1}")



                                                            #increase the count by 1
                                                            i=i+1
                                                            l=1 
                                                        except Exception as e:
                                                            if l<=20:
                                                        #wget is not be able to download because server is not allowing to downloaqd using wget
                                                                print("Json file will not be able to extract",e,url_new1)
                                                                l=l+1
                                                                continue
                                                            else:
                                                                remove_dir_if_not_empty(pt1)
                                                                l=1
                                                                break
                                                            
                                                    except Exception as e:
                                                        #if base64 libary is fail to extraact the url from the data key's value
                                                        if h<=20:
                                                            #if base64 libary is fail to extraact the url from the data key's value
                                                            print("Base64 decoder is nort working properly for get api 2 having file count more than or equal to 10 or reponse2",e)
                                                            h=h+1
                                                            continue
                                                        else:
                                                            remove_dir_if_not_empty(pt1)
                                                            h=1
                                                            break

                                                except Exception as e:
                                                    #if 2nd get api is not responding 
                                                    if g<=20:
                                                        #if 2nd get api is not responding 
                                                        print(f"2nd get api is not responding at {formatted_date} and file number {i}",e)
                                                        g=g+1
                                                        continue
                                                    else:
                                                        remove_dir_if_not_empty(pt1)
                                                        g=1
                                                        break
                                    
                                                
                                            else:
                                                #url for 2nd get api and for 'i' value is greatyer than equal to 10 (file number)
                                                url_new1=f"https://boapi.internal.gst.gov.in/govtapi/v2.0/common?action=FILEDET&type=R2B&state_cd=01&date={formatted_date}&file_num={i}"

                                                #auth-token variable is updated in header2 dictionary
                                                header2["auth-token"]=token
                                                try:
                                                    
                                                    #Calling 2nd get API 
                                                    response2=requests.get(url_new1,headers=header2,stream=True)
                                                    # 'data' key's value is store in 'encoding_string1'
                                                    encoding_string1=response2.json()["data"]
                                                    g=1

                                                    try:
                                                        
                                                        #Extracting the downloading url from the 'encoding_string1' variable value using base64 library
                                                        decode_bytes1=base64.b64decode(encoding_string1)
                                                        decoded_string1=decode_bytes1.decode('utf-8')
                                                        download_url=json.loads(decoded_string1)["url"]
                                                        h=1
                                                        try: 
                                                            
                                                            #download the .tar.gz file from the url and save it in the given folder    
                                                            # wget.download(download_url,out=os.path.join(f"{pt1}",f"{formatted_date}_{i}"+".tar.gz"))
                                                            response=requests.get(download_url,stream=True)
                                                            fileobj=io.BytesIO(response.content)
                                                            with tarfile.open(fileobj=fileobj,mode="r:gz") as tar:
                                                                tar.extractall(f"{pt1}")

                                                            #increase the count by 1
                                                            i=i+1
                                                            l=1
                                                        except Exception as e:
                                                            if l<=20:
                                                        #wget is not be able to download because server is not allowing to downloaqd using wget
                                                                print("Json file will not be able to extract",e,url_new1)
                                                                l=l+1
                                                                continue
                                                            else:
                                                                remove_dir_if_not_empty(pt1)
                                                                l=1
                                                                break
                                                                
                                                                

                                                    except Exception as e:
                                                        if h<=20:
                                                            #if base64 libary is fail to extraact the url from the data key's value
                                                            print("Base64 decoder is nort working properly for get api 2 having file count more than or equal to 10 or reponse2",e)
                                                            h=h+1
                                                            continue
                                                        else:
                                                            remove_dir_if_not_empty(pt1)
                                                            h=1
                                                            break
                                                            
                                                                
                                                            
                                                        
                                                except Exception as e:
                                                    if g<=20:
                                                        #if 2nd get api is not responding 
                                                        print(f"2nd get api is not responding at {formatted_date} and file number {i}",e)
                                                        g=g+1
                                                        continue
                                                    else:
                                                        remove_dir_if_not_empty(pt1)
                                                        g=1
                                                        break
                                                            
                                                            
                                                    

                                        except Exception as e:
                                            if f<=20:
                                                # This date contain no .tar.gz file
                                                print("Error in authentication or POST api is not responding",e)
                                                f=f+1
                                                continue
                                            else:
                                                remove_dir_if_not_empty(pt1)
                                                f=1
                                                break
                                                    #calling 2nd API fail
                                                    
                                                    
                                    if f>20 or g>20 or h>20 or l>20:
                                        current_date+=timedelta(days=1)
                                    else:
                                    #All the file at a particular date is downloaded           
                                        print(formatted_date,token,datetime.now().strftime("%H:%M:%S"),file_number)
                                        #current_date value increased by 1
                                        current_date+=timedelta(days=1)
                                    
                                except Exception as e:
                                    if e<=20:
                                        # This date contain no .tar.gz file
                                        print("Base64 decoder is not working properly of get api 1 or response1",e)
                                        e=e+1
                                        continue
                                    else:
                                        remove_dir_if_not_empty(pt1)
                                        current_date+=timedelta(days=1)
                                        e=1
                                        continue
                                    #Base64 libarary is not wortking properly to extract the file count
                                        
                            

                            except Exception as e :
                                #Date is increased by 1 day
                                if d<=20:
                                #This date contain no .tar.gz file
                                    print("error dure to some reason",formatted_date,token,datetime.now().strftime("%H:%M:%S"),e)
                                    d=d+1
                                    continue
                                else:
                                    remove_dir_if_not_empty(pt1)
                                    current_date+=timedelta(days=1)
                                    d=1
                                    continue

        

                        except Exception as e:
                            if c<=20:
                            #No response from the 1 get api 
                                print("Response from get Api 1 is not coming",e)
                                c=c+1
                                continue
                            else:
                                remove_dir_if_not_empty(pt1)
                                current_date+=timedelta(days=1)
                                c=1
                                continue


                    except Exception as e:
                        if b<=20:
                            print("Error in autherntication or POST api is not responding",e)
                            b=b+1
                            continue
                        else:
                            remove_dir_if_not_empty(pt1)
                            current_date+=timedelta(days=1)
                            b=1
                            continue


                        
                        #token is not generated again aftere thte while loop of date is generated
                        


                else:
                    current_date=current_date+timedelta(days=1)
                    continue

            break

        except Exception as e:
            # at most 6 time authentication api not respose, after that infinit while loop stops.
            if a<=6:
                if isinstance (e,KeyError):
                    print("Password may expire",e)
                    a=a+1
                    # infinite while loop continue if a<=6
                    continue
                else:
                    print("Error in autherntication or POST api is not responding",e)
                    a=a+1
                    # infinite while loop continue if a<=6
                    continue

            else:
                #infinite while loop breaks if a>7
                break

    
    for date in lt:
        lts=[]
        if date in os.listdir(pt) and len(os.listdir(f"{pt}/{date}")) !=0:
            pt3=Path(f"{pt}/{date}")
            arr = os.listdir(f'{pt3}')
            for dir in arr:
                for file1 in os.listdir(f"{pt3}\\{dir}"):
                    lts.append(f"{pt3}\\{dir}\\{file1}")
            time.sleep(10)
            print("pool 2 starts")
            num_cpu=multiprocessing.cpu_count()
            print(num_cpu)
            pool2=multiprocessing.Pool(processes=num_cpu-5)
            result=pool2.map(parse,lts)
            time.sleep(10)
            print("data frame making ")
            col_df=pd.DataFrame(result)
            print("dataframe made")
            print(len(col_df))
            time.sleep(5)
            pool2.close()
            pool2.join()
            time.sleep(5)
            print("date and time assign")
            col_df["rtnprd"]=pd.to_datetime(col_df["rtnprd"])
            col_df["gendt"]=pd.to_datetime(col_df["gendt"])
            

            remains=[]
            error=[]
            i=0
            while True:
                try:

                    conn = psycopg2.connect(
                        dbname="Commercial_Tax",
                        user="postgres",
                        password="Postgresql",
                        host="localhost",
                        port="5432"
                    )
            # Connect to the PostgreSQL database
                    while i < len(col_df):
                    # Create a cursor object
                        try:
                            cursor = conn.cursor()

                            # Define the data to be inserted as a list
                            data_to_insert = convert_types(list(col_df.iloc[i]))

                            # Construct the SQL query for insertion
                            sql_query = "INSERT INTO gstr2b_summary VALUES (%s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s,\
                                                                            %s, %s, %s, %s,%s,%s, %s, %s, %s,%s)"
                            
                            cursor.execute(sql_query, data_to_insert)

                            # Commit the transaction
                            conn.commit()

                            # Close cursor and connection
                            cursor.close()
                            i=i+1
                            

                        except Exception as e:
                            remains.append(i) 
                            error.append([i,e])
                            print("Error at",i," ",e)
                            
                            i=i+1
                            conn.close()
                            break

                        
                except:
                    conn.close()
                    continue
                
                if i >= len(col_df):
                    break

            col_df.iloc[remains].to_excel(Path(f"C:/Users/HP/vscode/R2B return code/Error and mismatch/mismatch R2B {date}.xlsx"))
            df_error=pd.DataFrame(error,columns=["index","error"])
            df_error.to_excel(Path(f"C:/Users/HP/vscode/R2B return code/Error and mismatch/error R2B {date}.xlsx"))

