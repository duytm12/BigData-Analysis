# BigData-Analysis
A live-streaming platform analyzed customer behavior data from April 1st, 2022 – April 30th, 2022. Purposes: Gain more insights on customers’ behavior Improve customer services for different customer segmentations.

I. Data Schema:
  1/ Input: OLTP data: JSON format
    _source.Contract:	Customer ID
	  _source.Mac:	Mac Address
	  _source.TotalDuration:	Amount of time customers used the service (in seconds)
	  _source.AppName:	Name of service which customers used

  2/ Output: OLAP data: CSV format
    Contract:	Customer ID	
    ChildDuration:	Amount of time customers watched Child content (in seconds)	- SUM all Duration for Child
    MovieDuration:	Amount of time customers watched Movie content (in seconds) -	SUM all Duration for Movie
    SportDuration:	Amount of time customers watched Sports content (in seconds)	- SUM all Duration for Sport
    TVDuration:	Amount of time customers watched TV content (in seconds) - SUM all Duration for TV
    RelaxDuration:	Amount of time customers watched Relax content (in seconds)	- SUM all Duration for Relax
    MostWatch:	Which content was watched the most	- Get the maximum Duration 
    Taste:	Combine all the contents that were watched	- Concatenate the Content
    DateCount:	How many days customers used service	- COUNT Contract
    Activeness:	% days used in a month	- DateCount / 30 * 100


II. Steps:
  1/ Extract data from Data Lake: Write code that allows the user to choose the date to get data 
  
  2/ Transform data:
    2.1/ Select Contract, AppName, and TotalDuration only
    2.2/ Data Mapping 
      AppName 	      Category
      KPLUS, CHANNEL	TV
      RELAX	          Relax
      CHILD	          Child
      SPORT	          Sport
      MOVIE	          Movie
    2.3/ Pivot data by Contract, SUM TotalDuration
    2.4/ Get MostWatch: Take the MAX value among all columns (TVDuration, RelaxDuration, ChildDuration, SportDuration, and MovieDuration) to know which content is most watch
    2.5/ Get Taste: Concate the Content which has TotalDuration >0 (Retrieve all the content a customer used)
    2.6/ Get DateCount: Count how many times a contract appears in data to see how many days customers have used the service.
    2.7/ Get Activeness: DateCount / 30 *100 
    
  3/ Save output: Save output as a CSV file on the local machine for storage purposes
  
  4/ Load output to Data Warehouse (MySQL):
    4.1/ Create a table in MySQL (Data Warehouse) to store OLAP output with appropriate column name and data type
    4.2/ Write code to connect to MySQL and import data to a Data Warehouse
    
  5/ ELT data:
    5.1/ Create a table in MySQL (Data Mart) to get daily_statistics based on OLAP output
    5.2/ Create Procedure for ELT data
    5.3/ Set Event Scheduler to automate the ELT process
    
  6/ Visualize data:
    6.1/ Load data into the Power BI desktop using the Direct Query method
    6.2/ Create Formatting columns: Segment customer based on Date Count
      DateCount	    Category	
      1 - 6 days	  At Risk Customer	
      7 - 13 days	  Need Attention Customer
      14 - 20 days	Valuable Customers
      21 - 30 days	Loyal Customers
    6.3/ Calculate some basic metrics: 
      TotalDuration	= SUM of all Duration
      AvgDurationPerPersonPerMonth	= TotalDuration / TotalUsers
      AvgDurationPerPersonPerWeek	= AvgDurationPerPersonPerMonth / 4
      AvgDurationPerPersonPerDay	= AvgDurationPerPersonPerWeek / 7
    6.4/ Generate Overview Dashboard
    6.5/ Generate detailed Analysis Dashboard
    6.6/ Publish Report to Power BI service to share a report and set auto-refresh every day to get a real-time dashboard
    
  7/ Generate project report


III. Tools:
- Scala (ETL), Spark (RDD, HDFS)
- MySQL (DDL, ELT, Store Procedure, Event Scheduler), MongoDB
- Power BI Desktop, Power BI service 
- PowerPoint, Excel
