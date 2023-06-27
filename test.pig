-- LOAD DATA
raw_ratings = LOAD 'ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
raw_users = LOAD 'users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
raw_movies = LOAD 'movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

-- GROUP BY OCCUPATION
group_users_occupation = GROUP raw_users BY occupation;
users_occupation_count = FOREACH group_users_occupation GENERATE group AS occupation, COUNT(raw_users.userID) AS count;
users_occupation_sorted = ORDER users_occupation_count BY count DESC;

-- OCCUPATION NAMES
occupation_names = LOAD 'occupation_names.dat' USING PigStorage('\t') AS (occupation, name);

-- JOIN RELATIONS AND DELETE OCCUPATION ID
occupation_joined_data = JOIN users_occupation_sorted BY occupation, occupation_names BY occupation;
occupation_filtered_data = FOREACH occupation_joined_data GENERATE name, count;

-- DUMP;
DUMP occupation_filtered_data;
