### Data Engineering Capstone Project

#### Project Summary

The idea of this project is to build ETL pipeline that aquires the I94 immigration data and city temperature to answer questions regarding average temperature and tourist arrivals to certian cities.

The project follows the follow steps:

- Step 1: Scope the Project and Gather Data
- Step 2: Explore and Assess the Data
- Step 3: Define the Data Model
- Step 4: Run ETL to Model the Data
- Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
In this project, I will create ETL pipeline that aquires and aggregates the I94 immigration data and average temperature date by destination city for April, 2016. I will use provided data sources to build stage tables. Next step will be to create fact table and necessary dimension tables. To create this solution, I will use Pandas, PySpark and AWS S3 buckets.

#### Describe and Gather Data 

* **I94 immigration data** - comes from the US National Tourism and Trade Office ([NTTO](https://www.trade.gov/national-travel-and-tourism-office)), provided by Udacity. The data is provided in [sas7bdat  binary file format](https://cran.r-project.org/web/packages/sas7bdat/vignettes/sas7bdat.pdf). I will use the following attributes:
    - i94cit - code of visitor origin country
    - i94port - code of destination city in the US
    - i94mode - code of transportation mode
    - arrdate - arrival date to the US
    - depdate - departure date from the US
    - i94visa - code of visa type 
    - visatype - class of admission legally admitting the non-immigrant to temporarily stay in US
    
    
* **City temperature data** - dataset comes from [Kaggle](https://www.kaggle.com/sudalairajkumar/daily-temperature-of-major-cities). The dataset is in [csv](https://docs.fileformat.com/spreadsheet/csv/) format. I will use the following attributes:
    - date
    - city
    - state
    - average temperature
