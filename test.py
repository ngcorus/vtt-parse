"""
The following class is used for testing, and is not used elsewere in the project.
"""

# import pandas as pd
# from io import BytesIO
# from dynamodb_json import json_util
# import csv
# import ast
# import json
import webvtt

class ReadFile:
    def readVTTFile(self):
        tempdata = []
        for caption in webvtt.read('./data/ABCS0054945880000032_1_WebVTT.vtt'):
            tempdata.append({"start":caption.start, "end":caption.end, "text":caption.text})
            # print(caption.start)
            # print(caption.end)
            # print(caption.text)
        print(tempdata)




readfile = ReadFile()
readfile.readVTTFile()