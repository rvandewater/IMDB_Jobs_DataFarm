# IMDB_Jobs_DataFarm
10 Flink jobs based on the following [Kaggle IMDb Dataset](https://www.kaggle.com/ashirwadsangwan/imdb-dataset/code). 
They are designed to provide a well rounded use of the complete dataset.
The following are rough descriptions of the semantics of these queries, with as second line the tables they use:
1. Q1: The sorted last name, birthyear, deathyear, age of all actors that have aged between 20 and 30
    1. name.basics
2. Q2: Get all sorted unoriginal transliterated greek titles, merged into one list by the amount of entries they have.
    1. title.akas
3. Q3:Get all the roles from actors, as well as the number of roles played
    1. title.principals, name.basics
4. Q4: Films/series produced before the 1950s with a rating of at least 8.5, with more than 10 reviews
    1. title.basics, title.ratings
5. Q5: Get titles after 1950, with a rating of at least 8.5, with more than 10 reviews that are german.
    1. title.basics, title.akas, title.ratings
6. Q6: Get all the actors of german movies, with the types of movie, ratings, year, years born/death , roles, jobs. 4 joins involved.
    1. title.akas, title.basics, title.principals, title.ratings, name.basics
7. Q7: Get the movie title, role, rating, name, year of birth/death of people that participated in a movie of at least rating 7
    1. title.akas, title.principals, title.ratings, name.basics
8. Q8: Get actors that are primarily known for films/series produced before 1970 with a rating of at least 8.5, with more than 10 reviews
    1.  title.basics, title.ratings, name.basics
9. Q9:  Get all the roles from actors and in which movies they played this role, as well as the number of roles played
    1. title.basics, title.principals, name.basics
10. Q10: Get all the actors details of german movies that contain archive footage, with the ratings, sorted by the title.
    1. title.akas, title.basics, title.principals, title.ratings
