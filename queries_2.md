- La del top 10
- La de película con género q la califica mejor
- La de películas peor rankeadas por mayores y mejor rankeades por jovenes

TOP 10 MOVIES FOR EACH OCCUPATION

occ_movie_avg_rating = FOREACH group_occ_movie GENERATE FLATTEN(group) AS (occupation, movieID), AVG(ratings_with_users_data.rating) AS avg_rating;

occ_movie_avg_rating_with_movies_data = JOIN occ_movie_avg_rating BY movieID, movie_data BY movieID;

occ_movie_title_avg_rating = FOREACH occ_movie_avg_rating_with_movies_data GENERATE occ_movie_avg_rating::occupation AS occupation, movie_data::title AS title, movie_data::year AS year, occ_movie_avg_rating::avg_rating AS avg_rating;

group_occ_movie_avg_rating = GROUP occ_movie_title_avg_rating BY occupation;


BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER)

movie_gender_avg_rating = FOREACH group_movie_gender_rating GENERATE FLATTEN(group) AS (title, year, gender), AVG(all_data.rating) AS avg_rating;

-- GENDER GIVING BEST AVERAGE RATING
all_top1_movie_gender_avg_rating = FOREACH group_movie_gender_avg_rating {
    sorted_movie_gender_avg_rating = ORDER movie_gender_avg_rating BY avg_rating DESC;
    top1_movie_gender_avg_rating = LIMIT sorted_movie_gender_avg_rating 1;
    GENERATE FLATTEN(top1_movie_gender_avg_rating);
};

...

all_top_movie_gender_avg_rating_ = JOIN all_top1_movie_gender_avg_rating BY (title, year), all_bott1_movie_gender_avg_rating BY (title, year);


WORST MOVIES WITH BEST SCORE BY MINOR AUDIENCES

group_data_age_below_18 = FILTER group_data_age_below_18_ BY COUNT(data_age_below_18) >=5;

data_age_below_18_avg_ratings = FOREACH group_data_age_below_18 GENERATE FLATTEN(group) AS (title,year), AVG(data_age_below_18.rating) AS avg_rating;

...

join_data_below_above_18 = JOIN data_age_below_18_avg_ratings BY (title,year), data_age_above_18_avg_ratings BY (title,year);

filter_join_data_below_above_18 = FILTER join_data_below_above_18 BY data_age_above_18_avg_ratings::avg_rating < data_age_below_18_avg_ratings::avg_rating;



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

WORST RATED-BY-EDUCATOR MOVIES WITH BEST SCORE BY COLLEGE/GRAD STUDENTS


PESIMISTIC AGE GROUPS (DESCENDING)

ratings_by_age_avg = FOREACH group_ratings_by_age GENERATE group AS age, AVG(ratings_with_users_data.rating) AS avg_rating;


YEAR OF BEST REVIEWED MOVIE FOR EACH AGE

age_movie_avg_ratings = FOREACH group_age_movie_ratings GENERATE FLATTEN(group) AS (age, title, year), AVG(age_movie_ratings.rating) AS avg_rating;
