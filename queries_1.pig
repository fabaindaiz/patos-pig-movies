
-- ####### FOR OCCUPATION

-- LOAD DATA
raw_ratings = LOAD 'ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
raw_users = LOAD 'users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
raw_movies = LOAD 'movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

-- GET OCCUPATION STATISTICS
group_occ = GROUP raw_users BY occupation;

occupation = FOREACH group_occ GENERATE group AS occupation, COUNT(raw_users.userID) AS count;

-- OCCUPATION ORDERED
occupation_ordered = ORDER occupation BY count DESC;


-- ####### FOR AGE

group_age = GROUP raw_users BY age;
age = FOREACH group_age GENERATE group AS age, COUNT(raw_users.userID) AS count;
age_ordered = ORDER age BY count DESC;

-- ####### FOR GENDER

group_gender = GROUP raw_users BY gender;
gender = FOREACH group_gender GENERATE group AS gender, COUNT(raw_users.userID) AS count;
gender_ordered = ORDER gender BY count DESC;

-- ####### FOR OCCUPATION AND GENDER

group_occ_gender = GROUP raw_users BY (occupation, gender);
occ_gender = FOREACH group_occ_gender GENERATE FLATTEN(group) AS (occupation,gender), COUNT(raw_users.userID) AS count;
occ_gender_ordered = ORDER occ_gender BY count DESC;

-- ####### FOR YEAR 

-- GET IDM EXTRACT YEAR SEQUENCE "(****)", REPLACE YEAR SEQUENCE AND TOKENIZE GENRES
movie_data = FOREACH raw_movies GENERATE movieID,
 REGEX_EXTRACT(title, '\\((\\d+)\\)$', 1) AS year,
 REPLACE(title, ' \\((\\d+)\\)$', '') AS title,
 TOKENIZE(genres, '|') AS genres;

group_year = GROUP movie_data BY year;
year = FOREACH group_year GENERATE group AS year, COUNT(movie_data.movieID) AS count;
year_ordered = ORDER year BY count DESC;

-- ####### FOR GENRES

movie_data_flatten = FOREACH movie_data GENERATE movieID, title, year, FLATTEN(genres) AS genres_flatten;
group_genres = GROUP movie_data_flatten BY genres_flatten;
genres = FOREACH group_genres GENERATE group AS genre, COUNT(movie_data_flatten.movieID) AS count;
genres_ordered = ORDER genres BY count DESC;

-- ####### FOR YEAR & GENRES

group_year_genres = GROUP movie_data_flatten BY (year, genres_flatten);
year_genres = FOREACH group_year_genres GENERATE FLATTEN(group) AS (year, genre), COUNT(movie_data_flatten.movieID) AS count;
year_genres_ordered = ORDER year_genres BY count DESC;

limit_ = LIMIT year_genres_ordered 10;

DUMP limit_;

