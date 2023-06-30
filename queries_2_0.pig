-- LOAD DATA
raw_ratings = LOAD 'hdfs://cm:9000/uhadoop2023/group14/ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
raw_users = LOAD 'hdfs://cm:9000/uhadoop2023/group14/users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
raw_movies = LOAD 'hdfs://cm:9000/uhadoop2023/group14/movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);
occupation_names = LOAD 'hdfs://cm:9000/uhadoop2023/group14/occupation_names.dat' USING PigStorage('\t') AS (occupation, name);

-- raw_ratings = LOAD 'ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
-- raw_users = LOAD 'users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
-- raw_movies = LOAD 'movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

-- OCCUPATION NAMES
-- occupation_names = LOAD 'occupation_names.dat' USING PigStorage('\t') AS (occupation, name);
users_occupation_name = JOIN raw_users BY occupation, occupation_names BY occupation;
users_no_occupation_id = FOREACH users_occupation_name GENERATE userID, gender, age, name, zipCode;


-- MOVIE DATA
movie_data = FOREACH raw_movies GENERATE movieID,
             REGEX_EXTRACT(title, '\\((\\d+)\\)$', 1) AS year,
             REPLACE(title, ' \\((\\d+)\\)$', '') AS title,
             TOKENIZE(genres, '|') AS genres;

-- RATING DATA (WITH >=5 SIGNIFICANCE)

group_raw_ratings = GROUP raw_ratings BY movieID;
filter_group_raw_ratings = FILTER group_raw_ratings BY COUNT(raw_ratings) >= 5;
ratings_data = FOREACH filter_group_raw_ratings GENERATE FLATTEN(raw_ratings);

-- JOIN RATINGS AND USERS WITH OCCUPATION NAMES INSTEAD OF ID
ratings_with_users_data = JOIN ratings_data BY userID, users_no_occupation_id BY userID;

-- JOIN ALL DATA FROM RAW RELATIONS
all_data = JOIN ratings_with_users_data BY ratings_data::raw_ratings::movieID, movie_data BY movieID;

-- GET USERS OCCUPATION, MOVIE TITLE, MOVIE YEAR, MOVIE RATING AND MOVIE GENRES FROM ALL_DATA
occ_movie_rating_genre = FOREACH all_data GENERATE occupation_names::name AS occupation, movie_data::title AS title, movie_data::year AS year, raw_ratings::rating AS rating, FLATTEN(movie_data::genres) AS genre;

-- ########################## WORST MOVIES WITH BEST SCORE BY MINOR AUDIENCES ##########################

data_age_below_18 = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::raw_users::age <= 18;
group_data_age_below_18_ = GROUP data_age_below_18 BY (movie_data::title,movie_data::year);
group_data_age_below_18 = FILTER group_data_age_below_18_ BY COUNT(data_age_below_18) >=5;

data_age_below_18_avg_ratings = FOREACH group_data_age_below_18 GENERATE FLATTEN(group) AS (title,year), AVG(data_age_below_18.rating) AS avg_rating;

data_age_above_18 = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::raw_users::age > 18;
group_data_age_above_18_ = GROUP data_age_above_18 BY (movie_data::title,movie_data::year);
group_data_age_above_18 = FILTER group_data_age_above_18_ BY COUNT(data_age_above_18)>=5;

data_age_above_18_avg_ratings = FOREACH group_data_age_above_18 GENERATE FLATTEN(group) AS (title,year), AVG(data_age_above_18.rating) AS avg_rating;

join_data_below_above_18 = JOIN data_age_below_18_avg_ratings BY (title,year), data_age_above_18_avg_ratings BY (title,year);

filter_join_data_below_above_18 = FILTER join_data_below_above_18 BY data_age_above_18_avg_ratings::avg_rating < data_age_below_18_avg_ratings::avg_rating;
sorted_join_data_below_above_18 = ORDER filter_join_data_below_above_18 BY data_age_above_18_avg_ratings::avg_rating ASC, data_age_below_18_avg_ratings::avg_rating DESC;
top5_data_below_above_18_ = LIMIT sorted_join_data_below_above_18 5;

top5_data_below_above_18 = FOREACH top5_data_below_above_18_ GENERATE data_age_below_18_avg_ratings::title AS title, data_age_below_18_avg_ratings::year AS year, data_age_below_18_avg_ratings::avg_rating AS rating_below_18, data_age_above_18_avg_ratings::avg_rating AS rating_above_18;

STORE top5_data_below_above_18 INTO '/uhadoop2023/group14/results/queries_2_0/top5_data_below_above_18';