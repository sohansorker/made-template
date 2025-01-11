# Project Plan

## Title
The Role of Infrastructure in Economic Growth
## Main Question

How the rate of road accident increasing day by day?

1. How does electricity production (kWh per capita) correlate with economic growth (GDP growth) in Latin American countries from 2009 to 2014?
   

2. What trends can be observed in electricity production and GDP growth for the selected Latin American countries during the study period, and are there any countries where changes in electricity production significantly impacted GDP growth?


## Description

I will search for the dataset and perform initial preprocessing, such as eliminating null values.

## Datasources

Datasource1: Worldbank (Electricity production (kWh per capita))

* Metadata URL:(https://data.worldbank.org/)
* URL:  "https://api.worldbank.org/v2/en/indicator/EG.USE.ELEC.KH.PC?downloadformat=csv"
* Data Type: Zip->CSV

* Description: This dataset contains the electricity production per capita for Latin American countries, which is a direct indicator of energy infrastructure.


Datasource2: Worldbank (GDP Growth (annual %))

* Metadata URL:(https://data.worldbank.org/)
* URL:  "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv"
* Data Type: Zip->CSV

* Description: This dataset measures the annual GDP growth (in constant prices) for the same countries, which reflects the economic performance over time.


## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1.	Dataset selection
2.	Building an automated data pipeline
3.	Exploratory Data Analysis (EDA).
4.	Reporting on findings
