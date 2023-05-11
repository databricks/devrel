## Best Practices on How to Process and Analyze Audit Logs with Delta Lake and Structured Streaming

2020-05-14 | [Watch the video](https://www.youtube.com/watch?v=hngJDSxQyjY) | This folder contains the presentation and sample notebooks

Cloud computing has fundamentally changed how companies operate - users are no longer subject to the restrictions of on-prem hardware deployments such as physical limits of resources and onerous environment upgrade processes. With the convenience and flexibility comes challenges on how to properly monitor how your users utilize these conveniently available resources. Failure to do so could result in problematic and costly anti-patterns.

This respository contains the following:
* A presentation that outlines why audit logs are important and why we chose to use Delta Lake / Structured Streaming to process them
* A notebook that details our ETL process for transforming the audit logs from their raw form into gold Delta Lake tables ready for analysis
* A notebook that walks through an example of how to use the transformed data
