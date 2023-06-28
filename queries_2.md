TOP 10 MOVIES FOR EACH OCCUPATION

occ_movie_avg_rating = FOREACH group_occ_movie GENERATE FLATTEN(group) AS (occupation, movieID), AVG(ratings_with_users_data.rating) AS avg_rating;

TOP GENRE FOR EACH OCCUPATION

occ_genre_avg_rating = FOREACH group_occ_genre GENERATE FLATTEN(group) AS (occupation,genre), AVG(occ_movie_rating_genre.rating) AS avg_rating;

TOP 5 MOVIES FOR EACH GENRE FOR EACH OCCUPATION

occ_movie_genre_avg_rating = FOREACH group_occ_movie_genre GENERATE FLATTEN(group) AS (occupation,title,genre), AVG(occ_movie_rating_genre.rating) AS avg_rating;

BEST AUDIENCE SCORE FOR EACH MOVIE (OCCUPATION)

movie_occ_avg_rating = FOREACH occ_movie_title_avg_rating GENERATE title AS title, year AS year, occupation AS occupation, avg_rating AS avg_rating;

BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER)



BEST AUDIENCE SCORE FOR EACH MOVIE (AGE)

movie_age_avg_rating = FOREACH group_movie_age_rating GENERATE FLATTEN(group) AS (title, year, age), AVG(all_data.rating) AS avg_rating;

BEST AUDIENCE SCORE FOR EACH MOVIE (OCCUPATION, GENDER)

movie_occ_gender_avg_rating = FOREACH group_movie_occ_gender_rating GENERATE FLATTEN(group) AS (title, year, occupation, gender), AVG(all_data.rating) AS avg_rating;

BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER, AGE)

movie_gender_age_avg_rating = FOREACH group_movie_gender_age_rating GENERATE FLATTEN(group) AS (title, year, gender, age), AVG(all_data.rating) AS avg_rating;

WORST MOVIES WITH BEST SCORE BY MINOR AUDIENCES



WORST RATED-BY-EDUCATOR MOVIES WITH BEST SCORE BY COLLEGE/GRAD STUDENTS



PESIMISTIC AGE GROUPS (DESCENDING)

ratings_by_age_avg = FOREACH group_ratings_by_age GENERATE group AS age, AVG(ratings_with_users_data.rating) AS avg_rating;

YEAR OF BEST REVIEWED MOVIE FOR EACH AGE

age_movie_avg_ratings = FOREACH group_age_movie_ratings GENERATE FLATTEN(group) AS (age, title, year), AVG(age_movie_ratings.rating) AS avg_rating;
