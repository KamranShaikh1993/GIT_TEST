from urllib.request import urlopen
from bs4 import BeautifulSoup
import os

import json
import time
import boto3
import logging
import yaml
from yaml.loader import SafeLoader
import requests
from boto3 import resources



logger = logging.getLogger()
logger.setLevel(logging.INFO)


timestr = time.strftime("%d-%m-%Y")
print(timestr)


with open('key_js.json') as f1:
    data = json.load(f1)
    a= data['aws_secret_access_key']
    print(a)

with open("key.yaml", "r") as f2:
    data = yaml.load(f2, Loader=SafeLoader)
    b = data['data']['aws_access_key_id']
    print(b)


# create a bucket       
client = boto3.client('s3',aws_access_key_id = b, 
                    aws_secret_access_key = a, 
                    region_name='ap-south-1')
response = client.list_buckets()
try:
    bucket_name = 'itvx'
    result = client.create_bucket(Bucket = bucket_name,
    CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})

    # target_bucket = 'itvx'
    # sub_folder = timestr   
    # client.put_object(Bucket = target_bucket, Key = sub_folder)
except:
    print('Buckert Already Exist')

print('2')

def func1():
    logger.info("Fetch the html file ")
    response = urlopen('https://www.itv.com/')
    html_doc = response.read()
 
    logger.info("Parse the html file ")
    soup = BeautifulSoup(html_doc, 'html.parser')

    logger.info("Format the parsed html file ")
    # Format the parsed html file
    strhtm = soup.prettify()

    logger.info("Selecting the required data ")
    index1 = strhtm.find('{"props":')
    index2 = strhtm.find('\n  </script>\n  <script src="')
    print(index1)
    print(index2)
    r1 = (strhtm[index1:index2])

    
    logger.info("Put json file file on s3 bucket ")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/your_file.json')

    s3object.put(
        Body=(bytes(json.dumps(r1).encode('UTF-8')))
    )

    logger.info("read json file from s3 bucket ")
    obj = s3.Object('itvx', f'{timestr}/your_file.json')
    data = json.load(obj.get()['Body'])

    r2 = json.loads(data)
    return r2






def func2(A):
    logger.info("Fetching data for heroContent ")
    # heroContent
    result_items = A['props']['pageProps']['heroContent']
    result_items = A['props']['pageProps']['heroContent']

    heroContent_1 = []
    i=0
    while i<len(result_items):
        heroContent_1.append(result_items[i])
        i+=1

    #print(heroContent_1)
    print( 'heroContent_1 :',len(heroContent_1))
    logger.info("Sending data for heroContent to S3 bucket")
    # put heroContent_1 json file on s3
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data1.json')

    s3object.put(
        Body=(bytes(json.dumps(heroContent_1).encode('UTF-8')))
    )

def func3(A):
    logger.info("Fetching data for Our_Top_Picks ")
    # Our_Top_Picks_1
    result_items1 = A['props']['pageProps']['editorialSliders']['editorialRailSlot1']['collection']['shows']

    Our_Top_Picks_1 = []
    i=0
    while i<len(result_items1):
        Our_Top_Picks_1.append(result_items1[i])
        i+=1

    # send json to s3
    logger.info("Sending json data for Our_Top_Picks to s3 Bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data2.json')

    s3object.put(
        Body=(bytes(json.dumps(Our_Top_Picks_1).encode('UTF-8')))
    )
    print('Our_Top_Picks_1 :',len(Our_Top_Picks_1))

def func4(A):
    logger.info("Fetching data for Smells_Like_Teen_Spirit ")
    
    result_items2 = A['props']['pageProps']['editorialSliders']['editorialRailSlot2']['collection']['shows']

    Smells_Like_Teen_Spirit_1 = []
    i=0
    while i<len(result_items2):
        Smells_Like_Teen_Spirit_1.append(result_items2[i])
        i+=1

    logger.info("Sending json data for Smells_Like_Teen_Spirit to S3 Bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data3.json')

    s3object.put(
        Body=(bytes(json.dumps(Smells_Like_Teen_Spirit_1).encode('UTF-8')))
    )

    print( 'Smells_Like_Teen_Spirit_1 :',len(Smells_Like_Teen_Spirit_1))


def func4(A):
    
    logger.info("Fetching data for Jonathan_Ross_guides_you_through_his_ITVX_film_picks ")
    result_items3 = A['props']['pageProps']['editorialSliders']['editorialRailSlot3']['collection']['shows']


    Jonathan_Ross_ITVX_film_picks_1 = []
    i=0
    while i<len(result_items3):
        Jonathan_Ross_ITVX_film_picks_1.append(result_items3[i])
        i+=1
        
        
    #send json to s3
    logger.info("Sending json data for Jonathan_Ross_guides_you_through_his_ITVX_film_picks to S3 bucket ")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data4.json')

    s3object.put(
        Body=(bytes(json.dumps(Jonathan_Ross_ITVX_film_picks_1).encode('UTF-8')))
    )

    print( 'Jonathan_Ross_guides_you_through_his_ITVX_film_picks :',len(Jonathan_Ross_ITVX_film_picks_1))


def func5(A):
    
    logger.info("Fetching data for Trending ")
    result_items4 = A['props']['pageProps']['trendingSliderContent']['items']

    Trending_1 = []
    i=0
    while i<len(result_items4):
        Trending_1.append(result_items4[i])
        i+=1
        
        
    #send json to s3
    logger.info("Sending json data for Trending to S3 Bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data5.json')

    s3object.put(
        Body=(bytes(json.dumps(Trending_1).encode('UTF-8'))))

    print( 'Trending_1 :',len(Trending_1))


def func6(A):
    logger.info("Fetching data for ITV_News ")
    result_items5 = A['props']['pageProps']['newsShortformSliderContent']['items']

    ITV_News_1 = []
    i=0
    while i<len(result_items5):
        ITV_News_1.append(result_items5[i])
        i+=1
        
    logger.info("Sending json data for ITV_News to S3 bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data6.json')

    s3object.put(
        Body=(bytes(json.dumps(ITV_News_1).encode('UTF-8'))))

    print('ITV_News :',len('ITV_News'))

def func7(A):
    logger.info("Fetching data for Just_In ")
    result_items6 = A['props']['pageProps']['editorialSliders']['editorialRailSlot4']['collection']['shows']


    Just_In_1 = []
    i=0
    while i<len(result_items6):
        Just_In_1.append(result_items6[i])
        i+=1
        
        
        logger.info("Sending json data for Just_In to S3 bucket")


    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data7.json')

    s3object.put(
        Body=(bytes(json.dumps(Just_In_1).encode('UTF-8'))))

    print('Just_In_1 :',len('Just_In_1'))


def func8(A):
    logger.info("Fetching data for Unmissasble_Boxsets ")

    result_items7 = A['props']['pageProps']['editorialSliders']['editorialRailSlot5']['collection']['shows']

    Unmissasble_Boxsets_1 = []

    i=0
    while i<len(result_items7):
        Unmissasble_Boxsets_1.append(result_items7[i])
        i+=1
        
    logger.info("Sending json data for Unmissasble_Boxsets to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data8.json')

    s3object.put(
        Body=(bytes(json.dumps(Unmissasble_Boxsets_1).encode('UTF-8'))))

    print('Unmissasble_Boxsets_1 :',len('Unmissasble_Boxsets_1'))

def func9(A):
    
    logger.info("Fetching data for ITV_Premium_Retro_Kids ")
    result_items8 = A['props']['pageProps']['editorialSliders']['editorialRailSlot6']['collection']['shows']

    ITV_Premium_Retro_Kids_1 = []

    i=0
    while i<len(result_items8):
        ITV_Premium_Retro_Kids_1.append(result_items8[i])
        i+=1
        
        
    logger.info("Sending json data for ITV_Premium_Retro_Kids to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data9.json')

    s3object.put(
        Body=(bytes(json.dumps(ITV_Premium_Retro_Kids_1).encode('UTF-8'))))

    print(len(ITV_Premium_Retro_Kids_1))


def func10(A):
    logger.info("Fetching json data for BAFTA_Winning_Flims to S3 bucket")

    result_items9 = A['props']['pageProps']['editorialSliders']['editorialRailSlot7']['collection']['shows']

    BAFTA_Winning_Flims_1 = []

    i=0
    while i<len(result_items9):
        BAFTA_Winning_Flims_1.append(result_items9[i])
        i+=1
        
        
    # send json to s3
    logger.info("Sending json data for BAFTA_Winning_Flims to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data10.json')

    s3object.put(
        Body=(bytes(json.dumps(BAFTA_Winning_Flims_1).encode('UTF-8'))))

    print('BAFTA_Winning_Flims_1:',len(BAFTA_Winning_Flims_1))

def func11(A):
    # All_Rise_Court_Session
    logger.info("Fetching json data for All_Rise_Court_Session to S3 bucket")

    result_items10 = A['props']['pageProps']['editorialSliders']['editorialRailSlot8']['collection']['shows']

    All_Rise_Court_Session_1 = []

    i=0
    while i<len(result_items10):
        All_Rise_Court_Session_1.append(result_items10[i])
        i+=1
        
        
    logger.info("Sending json data for All_Rise_Court_Session to S3 bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data10.json')

    s3object.put(
        Body=(bytes(json.dumps(All_Rise_Court_Session_1).encode('UTF-8'))))
    print('All_Rise_Court_Session_1 :',len(All_Rise_Court_Session_1))

def func12(A):
    logger.info("Fetching json data for You_Nicked! to S3 bucket")

    result_items11 = A['props']['pageProps']['editorialSliders']['editorialRailSlot9']['collection']['shows']

    You_Nicked_1 = []

    i=0
    while i<len(result_items11):
        You_Nicked_1.append(result_items11[i])
        i+=1

    logger.info("Sending json data for You_Nicked! to S3 bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data11.json')

    s3object.put(
        Body=(bytes(json.dumps(You_Nicked_1).encode('UTF-8'))))

    print('You_Nicked_1 :',(len(You_Nicked_1)))

def func13(A):
    logger.info("Fetching json data for Feel_Good_and_Funny ")

    result_items12 = A['props']['pageProps']['editorialSliders']['editorialRailSlot10']['collection']['shows']

    Feel_Good_and_Funny_1 = []

    i=0
    while i<len(result_items12):
        Feel_Good_and_Funny_1.append(result_items12[i])
        i+=1
        
    logger.info("Sending json data for Feel_Good_and_Funny to S3 bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data12.json')

    s3object.put(
        Body=(bytes(json.dumps(Feel_Good_and_Funny_1).encode('UTF-8'))))

    print('Feel_Good_and_Funny_1 :',(len(Feel_Good_and_Funny_1)))


def func14(A):
    logger.info("Fetching data for Make_It_a_Movie_Night ")

    result_items13 = A['props']['pageProps']['editorialSliders']['editorialRailSlot11']['collection']['shows']

    Make_It_a_Movie_Night_1 = []
    i=0
    while i<len(result_items13):
        Make_It_a_Movie_Night_1.append(result_items13[i])
        i+=1
        
    logger.info("Sending json data for Make_It_a_Movie_Night to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data13.json')

    s3object.put(
        Body=(bytes(json.dumps(Make_It_a_Movie_Night_1).encode('UTF-8'))))

    print('Make_It_a_Movie_Night_1 :',len(Make_It_a_Movie_Night_1))

def func15(A):
    
    logger.info("Fetching json data for Reality_Check ")
    result_items14 = A['props']['pageProps']['editorialSliders']['editorialRailSlot12']['collection']['shows']

    Reality_Check_1 = []

    i=0
    while i<len(result_items14):
        Reality_Check_1.append(result_items14[i])
        i+=1
        
        
    logger.info("Sending json data for Reality_Check to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data14.json')

    s3object.put(
        Body=(bytes(json.dumps(Reality_Check_1).encode('UTF-8'))))

    print('Reality_Check_1 :',len(Reality_Check_1))

def func16(A):
    
    logger.info("Fetching json data for Out_of_This_World ")
    result_items15 = A['props']['pageProps']['editorialSliders']['editorialRailSlot13']['collection']['shows']

    Out_of_This_World_1 = []

    i=0
    while i<len(result_items15):
        Out_of_This_World_1.append(result_items15[i])
        i+=1
        
    logger.info("Fetching json data for Out_of_This_World ")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data15.json')

    s3object.put(
        Body=(bytes(json.dumps(Out_of_This_World_1).encode('UTF-8'))))

    print('Out_of_This_World_1 :',len(Out_of_This_World_1))


def func17(A):
    logger.info("Fetching  data for ITVX_Premium_Best_of_British_from_BritBox ")

    result_items16 = A['props']['pageProps']['editorialSliders']['editorialRailSlot14']['collection']['shows']

    ITVX_Premium_Best_of_British_from_BritBox_1 = []

    i=0
    while i<len(result_items16):
        ITVX_Premium_Best_of_British_from_BritBox_1.append(result_items16[i])
        i+=1
        
        
    logger.info("Sending  data for ITVX_Premium_Best_of_British_from_BritBox ")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data16.json')

    s3object.put(
        Body=(bytes(json.dumps(ITVX_Premium_Best_of_British_from_BritBox_1).encode('UTF-8'))))

    print('ITVX_Premium_Best_of_British_from_BritBox_1 :',len(ITVX_Premium_Best_of_British_from_BritBox_1))

def func18(A):
    
    logger.info("Fetching json data for Iconic_ITV ")
    result_items17 = A['props']['pageProps']['editorialSliders']['editorialRailSlot15']['collection']['shows']

    Iconic_ITV_1 = []

    i=0
    while i<len(result_items17):
        Iconic_ITV_1.append(result_items17[i])
        i+=1
        
        
    logger.info("Sending json data for Iconic_ITV to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data17.json')

    s3object.put(
        Body=(bytes(json.dumps(Iconic_ITV_1).encode('UTF-8'))))

    print('Iconic_ITV_1 :',len(Iconic_ITV_1))


def func19(A):
    logger.info("Fetching json data for Anime ")

    result_items18 = A['props']['pageProps']['editorialSliders']['editorialRailSlot16']['collection']['shows']

    Anime_1 = []

    i=0
    while i<len(result_items18):
        Anime_1.append(result_items18[i])
        i+=1
        
        
    logger.info("Sending json data for Anime to S3 bucket")
    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data18.json')

    s3object.put(
        Body=(bytes(json.dumps(Anime_1).encode('UTF-8'))))

    print('Anime_1 :',len(Anime_1))

def func20(A):
    logger.info("Fetching json data for ITVX_Premium_Comedy_Boxsets_from_BritBox ")
    result_items19 = A['props']['pageProps']['editorialSliders']['editorialRailSlot17']['collection']['shows']

    ITVX_Premium_Comedy_Boxsets_from_BritBox_1 = []

    i=0
    while i<len(result_items19):
        ITVX_Premium_Comedy_Boxsets_from_BritBox_1.append(result_items19[i])
        i+=1
        
        
    logger.info("Sending json data for ITVX_Premium_Comedy_Boxsets_from_BritBox to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data19.json')

    s3object.put(
        Body=(bytes(json.dumps(ITVX_Premium_Comedy_Boxsets_from_BritBox_1).encode('UTF-8'))))


    print('result_items19 :',len(result_items19))

def func21(A):
    logger.info("Fetching json data for The_Kids_Collection ")
    result_items20 = A['props']['pageProps']['editorialSliders']['editorialRailSlot18']['collection']['shows']

    The_Kids_Collection_1 = []

    i=0
    while i<len(result_items20):
        The_Kids_Collection_1.append(result_items20[i])
        i+=1
        
        
    logger.info("Sending json data for The_Kids_Collection in S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data20.json')

    s3object.put(
        Body=(bytes(json.dumps(The_Kids_Collection_1).encode('UTF-8'))))

    print('The_Kids_Collection_1 :',len(The_Kids_Collection_1))

def func22(A):
    logger.info("Fetching json data for All_Star_Telly ")
    result_items21 = A['props']['pageProps']['editorialSliders']['editorialRailSlot19']['collection']['shows']

    All_Star_Telly_1 = []

    i=0
    while i<len(result_items21):
        All_Star_Telly_1.append(result_items21[i])
        i+=1
        
        
    logger.info("Sending json data for All_Star_Telly to S3 bucket")

    s3 = boto3.resource('s3', aws_access_key_id=b, aws_secret_access_key=a)
    s3object = s3.Object('itvx', f'{timestr}/My_data21.json')

    s3object.put(
        Body=(bytes(json.dumps(All_Star_Telly_1).encode('UTF-8'))))

    print('All_Star_Telly_1 :',len(All_Star_Telly_1))

#if __name__=='__main__':
def lambda_handler(event,context):
    A = func1()
    func2(A)
    func3(A)
    func4(A)
    func5(A)
    func6(A)
    func7(A)
    func8(A)
    func9(A)
    func10(A)
    func11(A)
    func12(A)
    func13(A)
    func14(A)
    func15(A)
    func16(A)
    func17(A)
    func18(A)
    func19(A)
    func20(A)
    func21(A)
    func22(A)
  