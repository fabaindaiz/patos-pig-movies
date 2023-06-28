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

-- ########################## YEAR OF BEST REVIEWED MOVIE FOR EACH AGE ##########################

age_movie_ratings = FOREACH all_data GENERATE ratings_with_users_data::users_no_occupation_id::raw_users::age AS age, movie_data::title AS title, movie_data::year AS year, raw_ratings::rating AS rating;
group_age_movie_ratings_ = GROUP age_movie_ratings BY (age, title, year);
group_age_movie_ratings = FILTER group_age_movie_ratings_ BY COUNT(age_movie_ratings) >= 5; 

age_movie_avg_ratings = FOREACH group_age_movie_ratings GENERATE FLATTEN(group) AS (age, title, year), AVG(age_movie_ratings.rating) AS avg_rating;

group_age_movie_avg_ratings = GROUP age_movie_avg_ratings BY age;

-- GET TOP MOVIE GENRE FOR EACH OCCUPATION
all_top_age_movie_avg_ratings = FOREACH group_age_movie_avg_ratings {
    sorted_age_movie_avg_ratings = ORDER age_movie_avg_ratings BY avg_rating DESC;
    top_age_movie_avg_ratings = LIMIT sorted_age_movie_avg_ratings 1;
    GENERATE FLATTEN(top_age_movie_avg_ratings);
};

STORE all_top_age_movie_avg_ratings INTO '/uhadoop2023/group14/results/queries_2_0/all_top_age_movie_avg_ratings';