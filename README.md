# The Needle Drop Album Reviews
 Quick project to familiarize myself with the basics of airflow by taking a look at some of the internet's busiest music nerd's history.

 Csv with data review scores and spotify id's provided by Joseph Green via Kaggle: https://www.kaggle.com/datasets/josephgreen/anthony-fantano-album-review-dataset


# Limitations
 After removing the eps from the dataset and retrieving the genres I could from Spotify. The initial 3023 reviewed projects were reduced to 1599 albums. The significant reduction
 in data points was in large part due to the way in which Spotify stores genre tags, usually being very specific like white noise or lacking spaces in the name like electropop. Making it harder 
 to extract the underlying main genre without significantly increasing the execution time of the ETL pipeline. Therefore, I settled on my current pipeline version which can retrieve enough distinct and relavent 
 genres while keeping the execution time to roughly 30 minutes. 
