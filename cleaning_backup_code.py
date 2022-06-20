#import dependencies

import pandas as pd
import json
import numpy as np
import re #regular expressions

#this file is in python3 kernel (not pythondata) so filepath needs to be from computer, not just folder we opened jupyter notebook in
file_directory = "/Users/emarieswenson/Desktop/Classwork/Unit 3 Databases/Movies-ETL/"



#opening json
with open(f"{file_directory}wikipedia-movies.json", mode='r') as file:
    wiki_movies_raw = json.load(file)
    
len(wiki_movies_raw)

#looking at first 5 dictionaries in json file
#wiki_movies_raw[:5]

#looking at last 5 dictionaries in json file
#wiki_movies_raw[-5:]

#looking at a few records in the middle
#wiki_movies_raw[3000:3005]

#opening and reading csv movie files from kaggle.com into pd dataframe
#-----------------------------------------------------------------------------------

kaggle_metadata= pd.read_csv(f"{file_directory}movies_metadata.csv")

kaggle_metadata.head(10)

kaggle_metadata.tail(10)

#getting a random sample of the data 
kaggle_metadata.sample(10)

ratings = pd.read_csv(f"{file_directory}ratings.csv")

#ratings.groupby(ratings["movieId"]).count()



### Cleaning out any movies that don't have a director or imdb_link or that have "episodes"
#-------------------------------------------------------------------------------------------

#turn json into dataframe
wiki_movies_df = pd.DataFrame(wiki_movies_raw)
wiki_movies_df.head()

#make columns into list because it's too long
wiki_movies_df.columns.to_list()

#restricting movies to only entries with a director and imdb link using list comprehension
wiki_movies = [movie for movie in wiki_movies_raw
    if ('Directed by' in movie or 'Director' in movie) 
               and 'imdb_link' in movie 
               and 'No. of episodes' not in movie]

len(wiki_movies) #this took movies from 7311 to 7076

#turn json into dataframe
wiki_movies_df = pd.DataFrame(wiki_movies) #took columns from 193 to 75
wiki_movies_df.columns.to_list

#This is why it's easier to load the JSON in first and then convert it to a DataFrame. Instead of trying to 
#identify which columns in our DataFrame don't belong, we just remove the bad data points, and the bad columns 
#never get imported in.

wiki_movies_df

wiki_movies[0] #first dictionary in list of json object

###Finding alternative titles columns 
#-------------------------------------------------------------------------------------------

wiki_movies_df.columns.to_list()

#wiki_movies_df["Arabic"].notnull().sum() # only 2 movies have values for arabic column

#these are the two movies with values in arabic column
#wiki_movies_df[wiki_movies_df["Arabic"].notnull()]["url"]

#value_counts() is a quick, easy way to see what non-null values there are in a column.
#only 4 movies with values for hepburn
#wiki_movies_df["Polish"].value_counts()

#look through URLs to see which columns equate to alternative titles 
wiki_movies_df[wiki_movies_df["imdb_link"].notnull()]["url"]



#columns with alternative titles (20 of them): 
#Polish, Arabic, Romanized, Russian, Hebrew, Yiddish, Chinese, Cantonese, Simplified, Traditional, Literally, 
#Hepburn, Japanese, Original title, McCune–Reischauer, Revised Romanization, Hangul, French, Also known as, Mandarin


#Cleaning out the movies with alternative titles & consolidating similar column names 
#-------------------------------------------------------------------------------------------
#note: function names should be verbs describing what the function is doing
#note: if a function is inside another function, the inner function can only be called inside the outer function

#cleaning each movie individually in a function
def clean_movie(movie):  ####### WHY MAKE THIS A FUNCTION INSTEAD OF A FOR LOOP? WE"RE JUST GOING THROUGH THE MOVIES IN THE JSON? YOU BASICALLY HAVE TO CALL THE FUNCTION WITH A FOR LOOP LATER ANYWAYS? 
    #making copy of the dictionaries so that we make non-destructive edits
    movie = dict(movie) #movie is a new local object (variable only in function), #dict() is a constructor, which is a special function that creates new objects 
   
    #empty dictionary to hold alternative titles, and list of columns with alternative titles
    alt_titles = {}
    alt_title_columns = ['Polish', 'Arabic', 'Romanized', 'Russian', 'Hebrew', 'Yiddish', 'Chinese', 'Cantonese', 'Simplified', 'Traditional', 'Literally', 'Hepburn', 'Japanese', 'Original title', 'McCune–Reischauer', 'Revised Romanization', 'Hangul', 'French', 'Also known as', 'Mandarin']

    #loop through alternative title column names, see if they are in the movie object
    for key in alt_title_columns:
        if key in movie:
            #if the key is in the movie object, add value to the alternative titles dictionary and then remove the column
            alt_titles[key]=movie[key]
            movie.pop(key)
    #add alt_titles dict to movies object
    if len(alt_titles) >0: ######WHY DO THIS IN AN IF STATEMENT? IF IT'S OUT OF THE FOR LOOP, WON'T IT JUST HAPPEN?
        movie["alt_titles"] = alt_titles
    
    #second, inner function to make multiple director names into one director column, etc. 
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name) #take out old name and move it to new name column to consolidate names
    
    #calling second function
    change_column_name('Directed by', 'Director')
    change_column_name('Country of origin', 'Country')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Original language(s)', 'Language')#####Is this one ok?
    change_column_name('Original release', 'Release date')
    change_column_name('Released', 'Release date')
    change_column_name('Produced by', 'Producer')
    change_column_name('Producer(s)', 'Producer')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Written by', 'Writer(s)')
    change_column_name("Theme music composer", "Composer(s)")
    change_column_name("Music by", "Composer(s)")
    change_column_name("Adaptation by", "Writer(s)")
    
    
    return movie

#this is the list comprehension calling the function from the json wiki_movies, written in a for loop (just for practice, same as code just below)
#clean_movies = []
#for movie in wiki_movies:
#    clean_movies.append(clean_movie(movie))

#calling the function to go through all of the movies in the partially-cleaned json wiki_movies and make them into a list of dictionaries using list comprehension
clean_movies = [clean_movie(movie) for movie in wiki_movies]
#making list of dictionaries into a dataframe     
wiki_movies_df = pd.DataFrame(clean_movies)

len(wiki_movies_df.columns) #from 75 to 56 columns after cleaning out alternative titles
                            #from 56 to 38 columns after consolidating columns

sorted(wiki_movies_df.columns.to_list()) #alphabetical list of new columns

##################################
#3 columns in 8.3.5 were in the module (in bottom list) that shouldn't have been: Hebrew, McCune-Reischauer, Russian

###########also, why do we not have to do all the json things within the "with open" statement like we had to in the elections module?




#Removing duplicate rows
#---------------------------------------------------------------
#using regular expressions to take imdb link and get imdb id from it
#also, making sure that we only have 1 imdb id per movie, since we're merging on imdb id

#extracting imdb id from imdb link
wiki_movies_df["imdb_id"] = wiki_movies_df["imdb_link"].str.extract(r"(tt\d{7})")
wiki_movies_df["imdb_id"]

#drop any duplicates of imdb id
wiki_movies_df.drop_duplicates(subset=["imdb_id"], inplace=True)

len(wiki_movies_df) #this took row number from 7076 to 7033, didn't lose many movies

#looking at how many null values in each column (either of the following two rows work)
wiki_movies_df.isnull().sum()
[[column, wiki_movies_df[column].isnull().sum()] for column in wiki_movies_df]

#more practice with writing out list comprehension for code just above
#columns=[]
#null_count=[]
#for column in wiki_movies_df:
#    columns.append(column)
#    null_count.append(wiki_movies_df[column].isnull().sum())
#practice_dict = {"columns":columns, "null_count":null_count}                      
#practice_df = pd.DataFrame(practice_dict)
#practice_df

#trimming down dataframe to any columns that are less than 90% full
wiki_columns_to_keep=[column for column in wiki_movies_df 
 if wiki_movies_df[column].isnull().sum()<len(wiki_movies_df)*0.9]

#only keeping columns previously decided on 
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

wiki_movies_df

#Converting data to appropriate data type: Box office
#-----------------------------------------------------------------------------------------

#Starting with Box office data (currently a string, should be numeric)

#dropping NaN's from box office column
box_office = wiki_movies_df["Box office"].dropna()
box_office #from 7033 to 5485



#making function to use regular expressions to get box office data
#regular expressions only work on string variables, so first finding things in column that are not string
def is_not_a_string(x):
    return type(x) != str
box_office[box_office.map(is_not_a_string)]

#Same as 3 lines of code above, but using lambda function. Lambda functions are functions without names that return a value automatically 
box_office[box_office.map(lambda x: type(x)!= str)]
################difference between .map and .loc? Map replaces and .loc ??? How would you write this for .loc?



#of the non-string abnormal values in box office list, many are lists. We're concatinating the lists with .join, and putting spaces between the items in the list. .apply applies a function to a pandas object
box_office = box_office.apply(lambda x: " ".join(x) if type(x) == list else x ) 


#fixing forms of box_office amounts that are in ranges like $4.35-4.37 million to be $4.37 million
box_office = box_office.str.replace(r"\$.*[-—–](?![a-z])", "$", regex=True) #regex=True says that we can use regular expressions in the replace statement
#there are three dashes above because the dashes are different types https://en.wikipedia.org/wiki/Dash

#2 main forms of box_office amounts $123,456,789 and $123.4 million. 
form1 = r"\$\s*\d+\.?\d*\s*[mb]illi*on"
form2 = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi*on)"
matches_form_1 = box_office.str.contains(form1, flags=re.IGNORECASE, na=False) #3896 forms of type 1 (from .sum() at end)
matches_form_2 = box_office.str.contains(form2, flags=re.IGNORECASE, na=False) #1544 of type 2 



#figuring out what doesn't match the two forms:
box_office[~matches_form_1 & ~matches_form_2] #pandas element operators: ~ means "not", & means "and"

#extracting the two forms and converting values
#str.extract() methon takes in a regular expression string, but it returns a DataFrame where every column is the data that matches a capture group
#make regular expression to get an object that matches form 1 or form 2
box_office.str.extract(f"({form1}|{form2})")

#turning extracted string values into float values
def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan
    # if input is of the form $###.# million
    if re.match(r"\$\s*\d+\.?\d*\s*milli*on", s, flags=re.IGNORECASE):
        # remove dollar sign and " million"
        s = re.sub("\$|\s|[a-zA-Z]", "", s) #####WHY NO r HERE IN FRONT OF THE QUOTES?
        # convert to float and multiply by a million
        value = float(s) * 10**6
        # return value
        return value
    # if input is of the form $###.# billion
    elif re.match(r"\$\s*\d+\.?\d*\s*billi*on", s, flags=re.IGNORECASE):
        # remove dollar sign and " billion"
        s = re.sub("\$|\s|[a-zA-Z]", "", s)
        # convert to float and multiply by a billion
        value = float(s) * 10**9
        # return value
        return value
    # if input is of the form $###,###,###
    elif re.match(r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi*on)", s, flags=re.IGNORECASE):
        # remove dollar sign and commas
        s = re.sub("\$|,", "", s) 
        # convert to float
        value = float(s)
        # return value
        return value
    # otherwise, return NaN
    else:
        return np.nan
    
wiki_movies_df["box_office"]=box_office.str.extract(f"({form1}|{form2})", flags=re.IGNORECASE)[0].apply(parse_dollars)

wiki_movies_df["box_office"]

#dropping boxoffice column since we don't need it anymore
#inplace=true specifies the drop operation to be in same dataframe rather creating a copy of the dataframe after drop
wiki_movies_df.drop("Box office", axis=1, inplace=True)
#there's a key error if you try to drop this twice without running the previous columns since 'Box office has already been dropped'


#me personally figuring out how to call parse dollars function

parse_dollars("$125,000,000")

practice = ["$125,000,000", "$123.4 million", "$56.4 million"]
practice_s = pd.Series(practice)
practice_s


clean_s = practice_s.str.extract(f"({form1}|{form2})", flags=re.IGNORECASE)[0].apply(parse_dollars)

clean_s

#figuring out how to make a string from a list
some_list = ['One','Two','Three']
'Number'.join(some_list)

#Converting data to appropriate data type: Budget
#---------------------------------------------------------------------------------

#make a budget variable:
budget=wiki_movies_df["Budget"].dropna()

#finding elements of budget that are not strings (they're lists)
budget[budget.map(lambda x: type(x) != str)]

#convert any lists in budget values to a string
budget=budget.map(lambda x: " ".join(x) if type(x) == list else x)

#fixing forms of budget amounts that are in ranges like $4.35-4.37 million to be $4.37 million
budget = budget.str.replace(r"\$.*[-—–](?![a-z])", "$", regex=True)
#fixing boxes in budget amounts like "$34 [3] [4] million"
budget = budget.str.replace(r"\[\d+\]\s*", "")

#form 1: "$25 million" form 2: "$600,000", also "$6-8 million" "4,000,000 (estimated)"
form_one = r"\$\s*\d+\.?\d*\s*[mb]il*li*on"
form_two = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi*on)"

#finding strings that match each form
matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE, na=False)
matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE, na=False)
#list of items in budget that don't match either form
budget[~matches_form_one & ~matches_form_two]

#applying parse dollars function to budget
wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

#drop unneeded original budget column
wiki_movies_df.drop("Budget", axis=1, inplace=True)

wiki_movies_df.columns


#Converting data to appropriate data type: Release date
#---------------------------------------------------------------------------------

#make a variable that holds the non-null values of Release date in the DataFrame, converting any lists to strings
release_date = wiki_movies_df["Release date"].dropna().apply(lambda x: " ".join(x) if type(x) == list else x) #7033 to 7001

#different date forms:
#1. Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
type_1 = r"[A-Z][a-z]+\s\d{1,2},\s\d{4}"
#2. Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
type_2 = r"\d{4}.\d{2}.\d{2}"
#3. Full month name, four-digit year (i.e., January 2000)
type_3 = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}"
#4. Four-digit year
type_4 = r"\d{4}"

#extracting dates of different types, using pandas to infer dates from different date structures 
wiki_movies_df["release_date"]=pd.to_datetime(release_date.str.extract(f"({type_1}|{type_2}|{type_3}|{type_4})", flags=re.IGNORECASE)[0], infer_datetime_format=True)

#drop release date column
wiki_movies_df.drop("Release date", axis=1, inplace=True)


#Converting data to appropriate data type: Running time
#---------------------------------------------------------------------------------

#make a variable that holds the non-null values of Running time in the DataFrame, converting any lists to strings
running_time = wiki_movies_df["Running time"].dropna().apply(lambda x: " ".join(x) if type(x)== list else x) #7033 to 6894 items in running_time after dropping na's
running_time

#most times are in format "123 minutes or 123 min or 123 mins"
running_time.str.contains(r"^\d*\s*m", flags=re.IGNORECASE, na=False).sum() #this form accounts for 6877 items

#which items didn't match the one type we specified above?
running_time[running_time.str.contains(r"^\d*\s*m", flags=re.IGNORECASE, na=False) != True]

#accounting for a few times that follow the format "1hr 35min", adding on other form to end of it after or |
running_time_extract = running_time.str.extract(r"(\d+)\s*ho?u?r?s?\s*(\d*)|(\d*)\s*m") #capturing 3 groups so the output is 3 columns (hours, minutes, minutes of times only recorded in minutes) 

#this new DataFrame is all strings, so convert them to numeric values. Some captured strings may be empty, use the to_numeric() method
#also set the errors argument to 'coerce' to turn the empty strings into Not a Number (NaN), then use fillna() to change all the NaNs to zeros.
running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors="coerce")).fillna(0)

#convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero
wiki_movies_df["running_time"] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

#dropping previous Running time column
wiki_movies_df.drop('Running time', axis=1, inplace=True)


wiki_movies_df.columns

