# portal-utilization
### Scripts to calculate the utilization rate for datasets

The utilization rate is a weighted average of number of downloads per website visitor. This is calculated per dataset page then summed to get an overall number for the portal.

For each dataset page, we start by summing the number of certain kinds of S3 log events registered for all of the S3 objects linked on that page. The log events are stored in Keen. The log events we use are Get object requests coming from a user agent that is a web browser. We then divide this number by the number of unique visitors to that dataset page as tracked by Google Analytics. That gives us the average downloads per visitor. 

However, the average must be weighted for the number of S3 objects linked on a page because a page with more than one link could have more downloads. To weight the average, we multiply it by an inverted count of the number of links. Using an inverted count penalizes the pages with more links in relation to pages with fewer links.

Starting with FY2020, we will not need to use S3 log events to calculate this metric. We will instead use event tracking in Google Analytics that was set up to capture clicks of dataset links.

To run these scripts, you will need [Google Analytics API Python libraries](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py) as well as the [Keen API library](https://keen.io/docs/api/) and [ua parser](https://github.com/ua-parser/uap-python). You may also need a more updated copy of datasets.json, which is a [json file of all datasets](https://data.sandiego.gov/datasets.json) available on the Open Data Portal

