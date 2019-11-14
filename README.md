# MindConnectAPI

# How to Onboard an Agent Using this MindConnect Custom Agent in Python 3
##Note: This is a Python 3 program
The main prerequisite is having the on-boarding JSON file from MindSphere 3.0. Once you have this file, save it as a json file, in the project folder called SouthBoundTokens. This is for better organization of the underlying structure.
This token file I called Initial.json

1. In MindSphere, copy the **Configuration ID** of your agent. It's a serial number located in the Datamapping area that defines the data coming from your agent. You will need to manually key this when preparing to upload the data to MindSphere.
2. In the DataMapping area inside MindSphere variables, **you need to find the serial number that is assigned to each variable. You will need to manually create a dictionary to feed it into this script**
3. Start a virtual environment *(Not required, but recommended).* First, `cd` to the current working directory.

    `python -m pip install --upgrade pip`
    
    `python3 -m pip install --user virtualenv`
    
    `python3 -m virtualenv env`
    
    on Linux or MacOS: `source env/bin/activate`
    
    on Windows: `.\env\Scripts\activate`
    
4. Install the requirements using pip *(or use your favorite IDE for assistance)*

    (in case you haven't done so already):
    
    `python -m pip install --upgrade pip`

    This is required, for it gathers all the library it needs
    
    `pip install -r requirements.txt`
5. Open up the file called ***PumpImporter.py***. Customize the script seen at the bottom for your own Agent. In the data_source, as a string edit the current value with the Configuration ID in Step 1. For the Data Points from Step 2, use that to edit data_point_id_lookup_table and the dictionary/object of my_agent.data_to_dict 
6. Run `python PumpImporter.py`
7. If you need to return back into your virtual environment, go to your current working directory: 
    on Linux or MacOS: `source env/bin/activate`
    
    on Windows: `.\env\Scripts\activate`
    
**SouthboundCoreAPIs.py**
This is the Core API required to get the Southbound API working. There is no logic in this file. It is used to help segment the classes for future unit testing.

**ConstantUpload.py**
This script can be imported to another script and can constantly send data one point at a time to MindSphere.

**MaxingOutUpload.py**
This script will determine the largest message MindSphere can receive.

**SendingAnomalyData.py**
This script will run MindSphere Anomaly Detection API. As of right now, you need to provide it a token located at `TechnicalUserFolder\technicalUserToken.json`. This folder will be ignored, so there will not be any token reveal during a *git push*.

**SouthboundBulkUpload.py**
This script will send a large package to MindSphere
