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
body1={"action":"ACCESSTOKEN","username":"GSTG2G01","password":"VrfH5HzA583CuWGkn1wTKKjRzcdaMrTuOTSZ56z9HaHCdywrnvgbryZst8nxbfYIbDbNGQEZqKqYiDQVuaWc8uFax2fnCz5/tkdjcXLk3K2LIKnmSrNeognj/UC4mIXLRbDwKApg/89iOFq7Fa8muRE5piY53mBKttYkkll5JLHPYm/CeNB4rAZQ+LRtOFJDoDau4ZFW20Gde0rkjQ/eUDN1hTqwlCVHkY9xJbk2lJfpfUQjiviVQpeMXFRTYyKZ8XuwqcI1gthsRzmupXKbblsld8TX3DjornvlWZKHvcg8bWR2c7DRiYJn733WVyKmMdsoaGsDZnCYZW1hp/oZ1w==","app_key":"YjAniQDRLSuHFKnu3cPvTv/b1sKq8RVrbfh6y5ILGmlycd1SayNZvOtE/kiUcJEoqSySTYAph8Q3ntGGlWGIFr/ZU32sAcDGi93g7chJKMSZcCSTy42M9BRs9VKMkWKoVd2WiGjTsDjPZ/xbsgKnDScRLq1/6dbHl+Nn1lKHfDpSyS/QGJOt/oPevEB4y51HAOfSG37Nfv1SRto2wdcJeHhsRKtFhGCikNJtngCX/bAdraIyXQIT3KWe3f4TTMgGPuVKy691YDo2bBnw3KjaNCGBdLI+9y+knWVGvyiqviI1phyo49uul2tw2JvSxk06k+h71g7DFqzs7mbK53Wmfg=="}
header2={"clientid":"l7xx1770d53ccd16417c9252ba7ecde54b69",
"client-secret":"d54330e9f8c743db8772770bba1cc019",
"username":"GSTG2G01",
'state-cd':"01",
"Content-Type":"application/json"
}



pt=Path("E:/GSTR2B daily data 24-25")

from datetime import datetime, timedelta



if __name__=="__main__":
    start=datetime(year=2024,month=8,day=31).date()
    data_import_date=datetime.now().date()
    lt1=[]
    current=start
    while current<data_import_date:
        lt1.append(current)
        current=current+timedelta(days=1)

    lt2=[i.strftime("%d-%m-%Y") for i in lt1]


    lt3=os.listdir(pt)
    lt=list(set(lt2)-set(lt3))
    
    if len(lt)!=0:
    # start_date=datetime.strptime(min(lt), '%d-%m-%Y').date()
    # end_date=datetime.strptime(max(lt), '%d-%m-%Y').date()
        dates = [datetime.strptime(date, '%d-%m-%Y') for date in lt]

        start_date = min(dates)
        end_date = max(dates)
    else:
        sys.exit()





    # start_date=datetime(year=2024,month=8,day=21)
    # end_date=datetime(year=2024,month=8,day=26)

    def get_token():
        response=requests.post(url1,headers=header1,json=body1,stream=True)
        token=response.json()["auth_token"]
        print(token)
        return token

    def renew_token(token_expiration):
        if datetime.now()>=token_expiration:
            return get_token()
        else:
            return None
        
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

                if current_date.strftime("%d-%m-%Y") in lt:

                    pt1=Path(f"{pt}/{current_date.strftime("%d-%m-%Y")}")

                    if not os.path.exists(pt1):
                        os.mkdir(pt1)
                
                #Convert the current_date into the dd/mm/yyyy format.
                    formatted_date=current_date.strftime("%d-%m-%Y")

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
                                        if os.path.exists(pt1):
                                            os.rmdir(pt1)
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
                                                                if os.path.exists(pt1):
                                                                    shutil.rmtree(pt1)
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
                                                            if os.path.exists(pt1):
                                                                shutil.rmtree(pt1)
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
                                                        if os.path.exists(pt1):
                                                            shutil.rmtree(pt1)
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
                                                                if os.path.exists(pt1):
                                                                    shutil.rmtree(pt1)
                                                                l=1
                                                                break
                                                                
                                                                

                                                    except Exception as e:
                                                        if h<=20:
                                                            #if base64 libary is fail to extraact the url from the data key's value
                                                            print("Base64 decoder is nort working properly for get api 2 having file count more than or equal to 10 or reponse2",e)
                                                            h=h+1
                                                            continue
                                                        else:
                                                            if os.path.exists(pt1):
                                                                shutil.rmtree(pt1)
                                                            h=1
                                                            break
                                                            
                                                                
                                                            
                                                        
                                                except Exception as e:
                                                    if g<=20:
                                                        #if 2nd get api is not responding 
                                                        print(f"2nd get api is not responding at {formatted_date} and file number {i}",e)
                                                        g=g+1
                                                        continue
                                                    else:
                                                        if os.path.exists(pt1):
                                                            shutil.rmtree(pt1)
                                                        g=1
                                                        break
                                                            
                                                            
                                                    

                                        except Exception as e:
                                            if f<=20:
                                #This date contain no .tar.gz file
                                                print("Error in autherntication or POST api is not responding",e)
                                                f=f+1
                                                continue
                                            else:
                                                if os.path.exists(pt1):
                                                    shutil.rmtree(pt1)
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
                                #This date contain no .tar.gz file
                                        print("Base64 decoder is not working properly of get api 1 or respone1",e)
                                        e=e+1
                                        continue
                                    else:
                                        if os.path.exists(pt1):
                                            shutil.rmtree(pt1)
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
                                    if os.path.exists(pt1):
                                        shutil.rmtree(pt1)
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
                                if os.path.exists(pt1):
                                    shutil.rmtree(pt1)
                                current_date+=timedelta(days=1)
                                c=1
                                continue


                    except Exception as e:
                        if b<=20:
                            print("Error in autherntication or POST api is not responding",e)
                            b=b+1
                            continue
                        else:
                            if os.path.exists(pt1):
                                shutil.rmtree(pt1)
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
            
