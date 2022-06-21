# Movies-ETL

## Purpose of analysis: 

This analysis was for Amazing Prime Video, a streaming movies and tv shows platform. They want to predict the next popular low-budget video to put on their platform and need to prepare a dataset for analysis to analysts. This project performed ETL (extract, transform, load) to three datasets. 

# Actions performed in this project: 

The three starter tables were: wikipedia data on movies in a json format, kaggle data on movies in a .csv format, and a very large ratings csv file (also from kaggle) that rated movies. The three datasets were imported into pandas, cleaned, merged, and put into sql tables. The wikipedia and kaggle files were imported, cleaned, merged, and the merged file was then cleaned of duplicate columns. After, the ratings file was imported and data was grouped and pivoted to show ratings for each movie. Ratings data was then merged with the combined wikipedia and kaggle files. All of the code to clean the files was put into one large function to run at once in the ETL_create_database.ipynb file. 

# Results

Two files (raw ratings data and the combined wikipedia/kaggle file) were put into a sql table for further analysis. 