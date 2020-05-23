## Predictive Maintenance (PdM) on IoT Data for Early Fault Detection w/ Delta Lake

2020-04-23 | [Watch the video](https://www.youtube.com/watch?v=68zy_nSV8g0) | This folder contains the presentation and sample notebooks


<i> Note: </i> Multiple personas are involved along different points of a data pipeline

This demo is a multi-notebook approach keeping these personas in mind i.e. there is division of labor where Data Investigation tasks are demoed in separate notebooks to emphasize that you can be involved in a part or the whole pipeline depending on what you choose.

##### Notebook Organization:
<b>Include</b> - Is a notebook that is included in other notebooks that defines base parameters <br>
<i>If you wish to change organization of the file paths, set them here </i><br>

<b>Setup & Teardown</b> - Are companion notebook that have helper routines for setup & teardown <br>
<i>Run this to create/tear down the setup; It has instructions on cloud infrastructure setup</i>
  

* <b>1-Data Ingest</b> -  reads the incoming streaming data to land it in the bronze zone
  * <i>1a-Read Bronze</i> - is a companion notebook which simulates a persona downstream consuming data from bronze table
* <b>2-Data Refinement</b> - Is the following notebook that reads the incoming stream from bronze table, refines it and lands it into silver zone
  * <i>2a-Read Silver</i> - is a companion notebook which simulates a persona downstream consuming data from silver table
* <b>3-Data Rollup</b> - Is the following notebook that reads the incoming stream from silver table, refines it and lands it into gold zone
  * <i>3a-Read Gold</i> - is a companion notebook which simulates a persona downstream consuming data from silver table
