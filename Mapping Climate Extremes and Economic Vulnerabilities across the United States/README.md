==========
This README acts as a brief user guide for installing dependencies, setting up the environment, and executing the interactive visualization demo.


DESCRIPTION
-----------
The "team105final" directory contains the full submission for the project "Mapping Climate Extremes and Economic Vulnerabilities Across the United States," completed by Team 105 for the course CSE 6242.

The project consists of two main subdirectories:

1. DOC/  
   - Contains "team105report.pdf" (final written report)  
   - Contains "team105poster.pdf" (poster presentation)  
   - Team members: Utsab Mitra, Sam Bolig, Katheryn Sauvigne, Daniel Damiano Cano, and Jose Alejandro Rodriguez.

2. CODE/
	- data_pipeline/  
   		- Contains all Python (*.py) files used for data extraction, data cleaning, and data transformation.
	- D3_build/ 
   		- Contains a toy dataset (*.csv) for running the D3 visualization.
   		- Contains all HTML, CSS, and JavaScript files needed for the interactive D3 visualization.
	- requirements.txt


INSTALLATION
------------
To run the Python scripts and the D3 visualization, ensure that you have:

1. Python 3.8+ installed  
   Recommended: Python 3.10 or newer  
   You can confirm your version by running:  
       python --version

2. Required Python libraries  
   Install required packages from requirements.txt

   We recommend installing them using pip:

       pip install -r requirements.txt

   Note: Some modules listed (os, re, io, json, time, etc.) are part of Python’s standard library and require no installation.

3. Recommended development environment
   - VS Code, PyCharm, or Jupyter Notebook  
   - For VS Code, the "Python" and "Live Server" extensions are useful

4. Browser requirement for visualization
   - Latest version of Google Chrome is strongly recommended
   - Visualization uses D3.js version 5, which is imported over internet connection

No additional installation is needed for the HTML/JavaScript files beyond running them through a local HTTP server (described below).


EXECUTION
---------
To run the visualization, you must launch a local HTTP server because D3 blocks local file access when using the "file://" protocol. Running the HTML file directly by double-clicking will not work.

Follow the steps below:

----------------------------------------
A. Setting Up the Local Web Server
----------------------------------------

1. Open a terminal or command prompt
2. Navigate to the CODE directory:
       cd path/to/team105final/CODE
3. Start a simple local HTTP server (Python 3.x):
       python -m http.server 8000
4. You should see a message similar to:
       Serving HTTP on :: port 8000 (http://localhost:8000/) ...
5. Do NOT close this terminal, as it keeps the server running.

----------------------------------------
B. Opening the Visualization
----------------------------------------

1. Open Google Chrome
2. In the address bar, type:
       http://localhost:8000/
3. Locate and open the main HTML file for the visualization ("CSE6242 Map.html").
4. The interactive D3 visualization will load and allow you to:
   - Explore climate-related disasters across the U.S.
   - Filter by year, month, and disaster
   - View geospatial and temporal patterns derived from the toy dataset

----------------------------------------
C. Running the Visualization
----------------------------------------

1. When you open the HTML file for the visualization, you are met with three panes
	Controls
	Map
	Details
2. The Controls pane contains dropdown menus to select Parameters such as Weather Risk Rank, FEMA-Damage Ratio, FEMA Spending, etc. There is a timeline slider that extends from 2020 to 2025, and a Play/Pause button that automatically runs the slider forward in time.
3. The Map pane displays the map of United States of America. The map updates with colors indicating the magnitudes of data depending on the menus selected from the dropdown. A legend describing the magnitude dynamically updates based on the dropdown menus.
4. The Details pane displays information when the mouse is hovered over the counties.
5. The Map has zoom in/zoom out functionality by single clicking and double clicking respectively.

----------------------------------------
D. Running the Python Data Processing Code (Optional)
----------------------------------------

If you wish to regenerate the datasets:

1. Open VS Code or another Python IDE
2. Open the CODE directory
3. Run the main Python data extraction script
       make_weather_risk_index.py  
4. The script will output CSV files to the same directory, which the visualization then reads automatically.

NOTE: You will need the datasets to run certain files which are not provided in the zip file. Instead two toy datasets "resilience_2000.csv", "resilience_2001.csv" are provided for visualization purposes.
NOTE: You will need to request an API Key from Census.gov to extract county populations: https://api.census.gov/data/key_signup.html

----------------------------------------

Your environment is now set up, and the full visualization is ready for use.
