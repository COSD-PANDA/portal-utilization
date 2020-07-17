# portal-utilization
### Scripts to calculate the utilization rate for datasets

The utilization rate is a weighted average of number of downloads per website visitor. This is calculated per dataset page then summed to get an overall number for the portal.

For each dataset page, we start by summing the number of certain kinds of S3 log events registered for all of the S3 objects linked on that page. The log events are stored in Keen. The log events we use are Get object requests coming from a user agent that is a web browser. We then divide this number by the number of unique visitors to that dataset page as tracked by Google Analytics. That gives us the average downloads per visitor. 

However, the average must be weighted for the number of S3 objects linked on a page because a page with more than one link could have more downloads. To weight the average, we multiply it by an inverted count of the number of links. Using an inverted count penalizes the pages with more links in relation to pages with fewer links.

Starting with FY2020, we will not need to use S3 log events solely to calculate this metric. We can also use event tracking in Google Analytics that was set up to capture clicks of dataset links. That has not been integrated into this calculation, although the file needed to do so is [here](http://seshat.datasd.org.s3.amazonaws.com/web_analytics/portal_events_datasd.csv).

To run these scripts, you will need [Google Analytics API Python libraries](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py) as well as the [Keen API library](https://keen.io/docs/api/) and [ua parser](https://github.com/ua-parser/uap-python).

If you are calculating for a new fiscal year, you will need to do the following:

- Add a folder to the files folder labeled fyXX

- Visit [https://data.sandiego.gov/datasets.json](https://data.sandiego.gov/datasets.json). I have been copy-pasting and creating a local copy of this and saving it into the fy folder. This is because what's available at the link changes every time the data portal is updated. I think it's best to use a copy that existed during the FY for which we're calculating metrics.

- Visit [https://data.sandiego.gov/data.json](https://data.sandiego.gov/data.json). Same as above.

The data.json file is the official open data catalog file that conforms to the Project Open Data schema. The datasets.json file is a different json of all dataset pages that includes the page path. The combination of these allows us to link the S3 object link to the Google Analytics page paths. There is an additional old-file-lookup.csv that manually fixes some page paths and download links that changed during the fiscal year. See instructions below for how to create this for a new fiscal year.

These are the steps to calculate the metric:
1. Update the fiscal year variable in both scripts.
2. Run the keen_analytics.py script. If you need to create old-file-lookup.csv, you will need to run this script twice. The first time, you'll be building the old-file-lookup.csv. Uncomment lines 209-213 and line 250 and comment out lines 222 - 249. The second time, you'll have the old-file-lookup created, so you can re-comment lines 209-213 and line 250 and uncomment lines 222-249. Is this a good way to do this? No. Anyway, make old-file-lookup by manually filling in the page_path_2. You can do this by referring to data.json and datasets.json for previous years. One output of this is dataset_downloads.csv, which is the number of keen events (result column) per S3 object per day, with the corresponding GA page path. Another output is dataset_page_links.csv.
3. Run the kpi-calc script. The overall utilization will print in the terminal, and the script will output portal-utilization.csv with the utilization per page. It also outputs other helpful files, listed below:

- portal-pages-months.csv has the number of pageviews and users per portal page (page_path_2 field) per month for this fiscal year time period.
	
- keen-drupal-months.csv has the number of times the web team imported one of the files of City docs links we're creating into our drupal sandiego.gov website. This is one of our critical jobs, and it shows how often the web team has relied on these files.

- keen-pages-ua.csv is the number of downloads per portal page (page_path_2 field) per day per source (browser, script, other). The script numbers have already subtracted most of the imports that happen when a portal page loads a data preview. This is somewhat flawed, as it simply takes the number of Python Requests and subtracts pageviews, but a user could click to display the preview of multiple files without creating a new pageview. In that case, the script numbers would be slightly higher.

Here are utilizations calculated so far
| Fiscal year  |  Utilization |
|--------------|--------------|
| 18 | 1.97 |
| 19 | 0.75 |
| 20 | 0.88 |
