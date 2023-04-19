# P3DD - Phase 3 Due Diligence
### Companies with clinical trials in phase 3 are high risk high reward investments. P3DD aims to mitigate this risk by compiling national clinical trial data for companies who are currently in phase 3 testing or have recently completed it. These companies are assigned to their Financial Instrument Global Identifier (FIGI) and registered name. This name is then used to parse Linkedin for daily job count updates. 
![Flowchart](https://github.com/TylerJSimpson/personal_project_clinicaltrials_2023/blob/main/P3DD_Flowchart.jpg)
___
### National Clinical Trial (NCT) data
The [AACT database](https://aact.ctti-clinicaltrials.org/) compiles all National Clinical Trial (NCT) data from [ClinicalTrials.gov](https://clinicaltrials.gov/) in a Postgres database with nightly refreshes and a weekly full rewrite.  
### Financial Instrument Global Identifier (FIGI) data
The [OpenFIGI API](https://www.openfigi.com/) is used to find and maintain the FIGI data linking the companies in clinical trials to their New York Stock Exchange (NYSE) CUSIP and Ticker.
### LinkedIn (Jobs) data
[Linkedin.com](https://www.linkedin.com/) is webscraped with Python to extract job information on the companies in clinical trials.
