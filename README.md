# LDA Analysis 3.0 - Topic Modelling through APACHE SPARK

- This is currently the 3rd version of LDA Analysis. 
- Mainly, we want to focus on extraction of topic terms and number of topics
- Note that this is an unsupervised training model involving a lot of review dataset 
in order for the the LDA Topic Outputs to be sufficient and accurate


Please be aware that you don't have to do anything in the code other than outputting

#### Installing Requirements:
Add configurations from 'requirements.txt'. If you're using an IDE, it will prompt you
to install the packages. Otherwise, simply run:
- _pip install <package_names>_


#### Expected prerequisites:
You should know how to run Apache Spark on Python IDE. Make sure Apache Spark
and Pypsark package (Python) is running properly before executing the program 


#### Before execution:
Make sure the following are existed in the program:
* 'review_info' folder (for outputs)
* 'topics' folder (for list of topics)
* 'data' folder (for the original raw data of reviews)

Then, run:
- _python report_output.py_

#### Warnings:
You may see a lot of WARNINGS and potentially SPARK error messages. Please do ignore them.
Note also that SPARK may run more than once. 


#### Resources:
- https://spark.apache.org/docs/latest/ml-clustering.html
- More will be updated here