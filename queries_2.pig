-- LOAD DATA
-- raw_ratings = LOAD 'hdfs://cm:9000/uhadoop2023/group14/ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
-- raw_users = LOAD 'hdfs://cm:9000/uhadoop2023/group14/users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
-- raw_movies = LOAD 'hdfs://cm:9000/uhadoop2023/group14/movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

raw_ratings = LOAD 'ratings_sample_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
raw_users = LOAD 'users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
raw_movies = LOAD 'movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

-- MOVIE DATA
movie_data = FOREACH raw_movies GENERATE movieID,
 REGEX_EXTRACT(title, '\\((\\d+)\\)$', 1) AS year,
 REPLACE(title, ' \\((\\d+)\\)$', '') AS title,
 TOKENIZE(genres, '|') AS genres;
 
-- JOIN RATINGS AND USERS
ratings = JOIN raw_ratings BY userID, raw_users BY userID;

-- ################################# TOP 10 MOVIES FOR EACH OCCUPATION ################################

-- GROUP BY OCCUPATION AND MOVIE
group_occ_ratings = GROUP ratings BY (raw_users::occupation, raw_ratings::movieID);

-- GET AVERAGE OF MOVIES FOR EACH OCCUPATION
avg_occ_ratings = FOREACH group_occ_ratings GENERATE FLATTEN(group) AS (occupation,movieID), AVG(ratings.rating) AS avg_rating;

avg_occ_movie_ratings_ = JOIN avg_occ_ratings BY movieID, movie_data BY movieID;

avg_occ_movie_ratings = FOREACH avg_occ_movie_ratings_ GENERATE avg_occ_ratings::occupation AS occupation, movie_data::title AS title, avg_occ_ratings::avg_rating AS avg_rating;

-- GET TOP10 ... SOLUTION FOUND IN https://stackoverflow.com/questions/17656012/pig-get-top-n-values-per-group

group_avg_occ_movie_ratings = GROUP avg_occ_movie_ratings BY occupation;

top10_avg_occ_movie_ratings = FOREACH group_avg_occ_movie_ratings {
    sorted_avg_occ_movie_ratings = ORDER avg_occ_movie_ratings BY avg_rating DESC;
    top_avg_occ_movie_ratings = LIMIT sorted_avg_occ_movie_ratings 10;
    GENERATE flatten(top_avg_occ_movie_ratings);
};


limit_ = LIMIT top10_avg_occ_movie_ratings 20;

DUMP limit_;

-- ################################# TOP GENRES FOR EACH OCCUPATION ################################
-- ########################## TOP 10 MOVIES FOR EACH GENRE FOR EACH OCCUPATION ##########################

