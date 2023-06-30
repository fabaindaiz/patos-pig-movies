-- LOAD DATA
raw_ratings = LOAD 'hdfs://cm:9000/uhadoop2023/group14/ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
raw_users = LOAD 'hdfs://cm:9000/uhadoop2023/group14/users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
raw_movies = LOAD 'hdfs://cm:9000/uhadoop2023/group14/movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);
occupation_names = LOAD 'hdfs://cm:9000/uhadoop2023/group14/occupation_names.dat' USING PigStorage('\t') AS (occupation, name);

-- raw_ratings = LOAD 'ratings_sample_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
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

-- ########################## PROPORTION OF VOTES FOR {OCUPPATION, GENDER, AGE} ###################

group_ratings_by_occupation = GROUP ratings_with_users_data BY users_no_occupation_id::occupation_names::name;
occupation_votes = FOREACH group_ratings_by_occupation GENERATE group AS occupation, COUNT(ratings_with_users_data) AS votes;

group_ratings_by_gender = GROUP ratings_with_users_data BY users_no_occupation_id::raw_users::gender;
gender_votes = FOREACH group_ratings_by_gender GENERATE group AS gender, COUNT(ratings_with_users_data) AS votes;

group_ratings_by_age = GROUP ratings_with_users_data BY users_no_occupation_id::raw_users::age;
age_votes = FOREACH group_ratings_by_age GENERATE group AS age, COUNT(ratings_with_users_data) AS votes;
--
group_ratings_by_gender_age = GROUP ratings_with_users_data BY (users_no_occupation_id::raw_users::gender,users_no_occupation_id::raw_users::age);
gender_age_votes = FOREACH group_ratings_by_gender_age GENERATE FLATTEN(group) AS (gender, age), COUNT(ratings_with_users_data) AS votes;

group_ratings_by_occupation_gender = GROUP ratings_with_users_data BY (users_no_occupation_id::occupation_names::name,users_no_occupation_id::raw_users::gender);
occupation_gender_votes = FOREACH group_ratings_by_occupation_gender GENERATE FLATTEN(group) AS (occupation, gender), COUNT(ratings_with_users_data) AS votes;

total_votes = GROUP occupation_votes ALL;
total_ = FOREACH total_votes GENERATE SUM(occupation_votes.votes) AS total;

proportion_votes_occ = FOREACH occupation_votes GENERATE occupation, (float)votes/total_.total AS vote_proportion;
proportion_votes_gender = FOREACH gender_votes GENERATE gender, (float)votes/total_.total AS vote_proportion;
proportion_votes_age = FOREACH age_votes GENERATE age, (float)votes/total_.total AS vote_proportion;

proportion_votes_gender_age = FOREACH gender_age_votes GENERATE gender, age, (float)votes/total_.total AS vote_proportion;
proportion_votes_occ_gender = FOREACH occupation_gender_votes GENERATE occupation, gender, (float)votes/total_.total AS vote_proportion;

-- ########################## MOST REVIEWED MOVIES ##########################

group_by_movies = GROUP raw_ratings BY movieID;
count_votes_movies_ = FOREACH group_by_movies GENERATE FLATTEN(group) AS movieID, COUNT(raw_ratings) AS count;

join_count_votes_movies_ = JOIN movie_data BY movieID, count_votes_movies_ BY movieID;
count_votes_movies = FOREACH join_count_votes_movies_ GENERATE movie_data::title, movie_data::year, count_votes_movies_::count;
sorted_count_votes_movies = ORDER count_votes_movies BY count DESC;

STORE proportion_votes_occ INTO '/uhadoop2023/group14/results/queries_3/proportion_votes_occ';
STORE proportion_votes_gender INTO '/uhadoop2023/group14/results/queries_3/proportion_votes_gender';
STORE proportion_votes_age INTO '/uhadoop2023/group14/results/queries_3/proportion_votes_age';
STORE proportion_votes_gender_age INTO '/uhadoop2023/group14/results/queries_3/proportion_votes_gender_age';
STORE proportion_votes_occ_gender INTO '/uhadoop2023/group14/results/queries_3/proportion_votes_occ_gender';
STORE sorted_count_votes_movies INTO '/uhadoop2023/group14/results/queries_3/sorted_count_votes_movies';

-- ########################## TEST ##########################
-- DEFINE LIMIT TO SEE FIRST 10 ROWS IN OUTPUT
-- output_limit = LIMIT sorted_count_votes_movies 10;
-- DUMP output_limit;
