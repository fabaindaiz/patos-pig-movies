-- LOAD DATA
raw_users = LOAD 'hdfs://cm:9000/uhadoop2023/group14/users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
raw_movies = LOAD 'hdfs://cm:9000/uhadoop2023/group14/movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

-- raw_users = LOAD 'users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
-- raw_movies = LOAD 'movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

-- #### USERS ####

-- GROUP BY OCCUPATION
group_users_occ = GROUP raw_users BY occupation;
users_occ_count = FOREACH group_users_occ GENERATE group AS occupation, COUNT(raw_users.userID) AS count;
users_occ_sorted = ORDER users_occ_count BY count DESC;

-- OCCUPATION NAMES
occupation_names = LOAD 'hdfs://cm:9000/uhadoop2023/group14/occupation_names.dat' USING PigStorage('\t') AS (occupation, name);
occ_joined_data = JOIN users_occ_sorted BY occupation, occupation_names BY occupation;
occ_filtered_data = FOREACH occ_joined_data GENERATE name, count;

-- GROUP BY AGE
group_users_age = GROUP raw_users BY age;
users_age_count = FOREACH group_users_age GENERATE group AS age, COUNT(raw_users.userID) AS count;
users_age_sorted = ORDER users_age_count BY count DESC;

-- GROUP BY GENDER
group_users_gender = GROUP raw_users BY gender;
users_gender_count = FOREACH group_users_gender GENERATE group AS gender, COUNT(raw_users.userID) AS count;
users_gender_sorted = ORDER users_gender_count BY count DESC;

-- GROUP BY OCCUPATION AND GENDER
group_users_occ_gender = GROUP raw_users BY (occupation, gender);
users_occ_gender_count = FOREACH group_users_occ_gender GENERATE FLATTEN(group) AS (occupation,gender), COUNT(raw_users.userID) AS count;
users_occ_gender_sorted = ORDER users_occ_gender_count BY count DESC;


-- #### MOVIES ####

-- GET IDM EXTRACT YEAR SEQUENCE "(****)", REPLACE YEAR SEQUENCE AND TOKENIZE GENRES
movie_data = FOREACH raw_movies GENERATE movieID,
             REGEX_EXTRACT(title, '\\((\\d+)\\)$', 1) AS year,
             REPLACE(title, ' \\((\\d+)\\)$', '') AS title,
             TOKENIZE(genres, '|') AS genres;

movie_data_flatten = FOREACH movie_data GENERATE movieID, title, year, FLATTEN(genres) AS genres_flatten;

-- GROUP BY YEAR
group_movie_year = GROUP movie_data BY year;
movie_year_count = FOREACH group_movie_year GENERATE group AS year, COUNT(movie_data.movieID) AS count;
movie_year_sorted = ORDER movie_year_count BY count DESC;

-- GROUP BY GENRES
group_movie_genres = GROUP movie_data_flatten BY genres_flatten;
movie_genres_count = FOREACH group_movie_genres GENERATE group AS genre, COUNT(movie_data_flatten.movieID) AS count;
movie_genres_sorted = ORDER movie_genres_count BY count DESC;

-- GROUP BY YEAR AND GENRES
group_movie_year_genres = GROUP movie_data_flatten BY (year, genres_flatten);
movie_year_genres_count = FOREACH group_movie_year_genres GENERATE FLATTEN(group) AS (year, genre), COUNT(movie_data_flatten.movieID) AS count;
movie_year_genres_sorted = ORDER movie_year_genres_count BY count DESC;

STORE occ_filtered_data INTO '/uhadoop2023/group14/results/queries_1/users_occ_sorted';
STORE users_age_sorted  INTO '/uhadoop2023/group14/results/queries_1/users_age_sorted' ;
STORE users_gender_sorted INTO '/uhadoop2023/group14/results/queries_1/users_gender_sorted' ;
STORE users_occ_gender_sorted INTO '/uhadoop2023/group14/results/queries_1/users_occ_gender_sorted';
STORE movie_year_sorted INTO '/uhadoop2023/group14/results/queries_1/movie_year_sorted';
STORE movie_genres_sorted INTO '/uhadoop2023/group14/results/queries_1/movie_genres_sorted';
STORE movie_year_genres_sorted INTO '/uhadoop2023/group14/results/queries_1/movie_year_genres_sorted'; 

-- DEFINE LIMIT TO SEE FIRST 10 ROWS IN OUTPUT
-- output_limit = LIMIT movie_year_genres_sorted 10;
-- DUMP output_limit;

