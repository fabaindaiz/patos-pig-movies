-- LOAD DATA
-- raw_ratings = LOAD 'hdfs://cm:9000/uhadoop2023/group14/ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
-- raw_users = LOAD 'hdfs://cm:9000/uhadoop2023/group14/users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
-- raw_movies = LOAD 'hdfs://cm:9000/uhadoop2023/group14/movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);
-- occupation_names = LOAD 'hdfs://cm:9000/uhadoop2023/group14/occupation_names.dat' USING PigStorage('\t') AS (occupation, name);

raw_ratings = LOAD 'ratings_sample_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
raw_users = LOAD 'users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
raw_movies = LOAD 'movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);
-- OCCUPATION NAMES
occupation_names = LOAD 'occupation_names.dat' USING PigStorage('\t') AS (occupation, name);

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


-- ################################# TOP 10 MOVIES FOR EACH OCCUPATION ################################

-- GROUP BY OCCUPATION AND MOVIE ID
group_occ_movie_ = GROUP ratings_with_users_data BY (occupation_names::name, ratings_data::raw_ratings::movieID);

-- GET AVERAGE RATING OF MOVIE FOR EACH OCCUPATION
group_occ_movie = FILTER group_occ_movie_ BY COUNT(ratings_with_users_data) >= 5;

-- GET AVERAGE RATING OF MOVIE FOR EACH OCCUPATION
occ_movie_avg_rating = FOREACH group_occ_movie GENERATE FLATTEN(group) AS (occupation, movieID), AVG(ratings_with_users_data.rating) AS avg_rating;

-- JOIN OCCUPATION_MOVIE_AVG_RATING AND MOVIES
occ_movie_avg_rating_with_movies_data = JOIN occ_movie_avg_rating BY movieID, movie_data BY movieID;

-- ADD MOVIE TITLE TO RELATION AND DELETE MOVIE ID
occ_movie_title_avg_rating = FOREACH occ_movie_avg_rating_with_movies_data GENERATE occ_movie_avg_rating::occupation AS occupation, movie_data::title AS title, movie_data::year AS year, occ_movie_avg_rating::avg_rating AS avg_rating;

-- GET TOP 10 MOVIES FOR EACH OCCUPATION
-- SOLUTION FOUND IN https://stackoverflow.com/questions/17656012/pig-get-top-n-values-per-group
group_occ_movie_avg_rating = GROUP occ_movie_title_avg_rating BY occupation;

all_top10_occ_movie_avg_rating = FOREACH group_occ_movie_avg_rating {
    sorted_occ_movie_avg_rating = ORDER occ_movie_title_avg_rating BY avg_rating DESC;
    top10_occ_movie_avg_rating = LIMIT sorted_occ_movie_avg_rating 10;
    GENERATE flatten(top10_occ_movie_avg_rating);
};


-- ################################# TOP GENRE FOR EACH OCCUPATION ################################

-- JOIN ALL DATA FROM RAW RELATIONS
all_data = JOIN ratings_with_users_data BY ratings_data::raw_ratings::movieID, movie_data BY movieID;

-- GET USERS OCCUPATION, MOVIE TITLE, MOVIE YEAR, MOVIE RATING AND MOVIE GENRES FROM ALL_DATA
occ_movie_rating_genre = FOREACH all_data GENERATE occupation_names::name AS occupation, raw_ratings::movieID AS movieID, movie_data::title AS title, movie_data::year AS year, raw_ratings::rating AS rating, FLATTEN(movie_data::genres) AS genre;

-- GROUP BY USERS OCCUPATION AND MOVIE GENRE
group_occ_genre_ = GROUP occ_movie_rating_genre BY (occupation, genre);
group_occ_genre = FILTER group_occ_genre_ BY COUNT(occ_movie_rating_genre) >= 5;

-- GET AVERAGE RATING OF MOVIE GENRE FOR EACH OCCUPATION
occ_genre_avg_rating = FOREACH group_occ_genre GENERATE FLATTEN(group) AS (occupation,genre), AVG(occ_movie_rating_genre.rating) AS avg_rating;

-- GROUP BY USERS OCCUPATION
group_occ_genre_avg_rating = GROUP occ_genre_avg_rating BY occupation;

-- GET TOP MOVIE GENRE FOR EACH OCCUPATION
all_top_occ_genre_avg_rating = FOREACH group_occ_genre_avg_rating {
    sorted_occ_genre_avg_rating = ORDER occ_genre_avg_rating BY avg_rating DESC;
    top_occ_genre_avg_rating = LIMIT sorted_occ_genre_avg_rating 1;
    GENERATE FLATTEN(top_occ_genre_avg_rating);
};


-- ########################## TOP 5 MOVIES FOR EACH GENRE FOR EACH OCCUPATION ##########################

-- GROUP BY USERS OCCUPATION AND MOVIES TITLE AND MOVIES GENRE
group_occ_movie_genre_ =  GROUP occ_movie_rating_genre BY (occupation, movieID, genre);
group_occ_movie_genre = FILTER group_occ_movie_genre_ BY COUNT(occ_movie_rating_genre) >= 5;

-- GET THE AVERAGE RATING OF EACH MOVIE BY OCCUPATION, SEPARATED BY GENRE TOO
occ_movie_genre_avg_rating = FOREACH group_occ_movie_genre GENERATE FLATTEN(group) AS (occupation,movieID,genre), AVG(occ_movie_rating_genre.rating) AS avg_rating;

-- GROUP BY USERS OCCUPATION
group_occ_movie_genre_avg_rating = GROUP occ_movie_genre_avg_rating BY occupation;

all_top5_occ_movie_genre_avg_rating = FOREACH group_occ_movie_genre_avg_rating {
    sorted_occ_movie_genre_avg_rating = ORDER occ_movie_genre_avg_rating BY avg_rating DESC;
    top5_occ_movie_genre_avg_rating = LIMIT sorted_occ_movie_genre_avg_rating 5;
    GENERATE flatten(top5_occ_movie_genre_avg_rating);
};

-- ########################## TOP MOVIE FOR EACH GENRE FOR EACH OCCUPATION ##########################

-- GROUP BY USERS OCCUPATION AND MOVIES TITLE AND MOVIES GENRE
group_occ_movie_genre_ =  GROUP occ_movie_rating_genre BY (occupation, movieID, genre);
group_occ_movie_genre = FILTER group_occ_movie_genre_ BY COUNT(occ_movie_rating_genre) >= 5;

-- GET THE AVERAGE RATING OF EACH MOVIE BY OCCUPATION, SEPARATED BY GENRE TOO
occ_movie_genre_avg_rating_ = FOREACH group_occ_movie_genre GENERATE FLATTEN(group) AS (occupation,movieID,genre), AVG(occ_movie_rating_genre.rating) AS avg_rating;
join_occ_movie_genre_avg_rating_ = JOIN movie_data BY movieID, occ_movie_genre_avg_rating_ BY movieID;
occ_movie_genre_avg_rating = FOREACH join_occ_movie_genre_avg_rating_ GENERATE occ_movie_genre_avg_rating_::occupation AS occupation, occ_movie_genre_avg_rating_::genre AS genre, movie_data::title AS title, movie_data::year AS year, occ_movie_genre_avg_rating_::avg_rating AS avg_rating;

-- GROUP BY USERS OCCUPATION AND MOVIE GENRE
group_occ_movie_genre_avg_rating = GROUP occ_movie_genre_avg_rating BY (occupation, genre);

all_top1_occ_movie_genre_avg_rating = FOREACH group_occ_movie_genre_avg_rating {
    sorted_occ_movie_genre_avg_rating = ORDER occ_movie_genre_avg_rating BY avg_rating DESC;
    top1_occ_movie_genre_avg_rating = LIMIT sorted_occ_movie_genre_avg_rating 1;
    GENERATE flatten(top1_occ_movie_genre_avg_rating);
};

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (OCCUPATION) ##########################

movie_occ_avg_rating = FOREACH occ_movie_title_avg_rating GENERATE title AS title, year AS year, occupation AS occupation, avg_rating AS avg_rating;
group_movie_occ_avg_rating = GROUP movie_occ_avg_rating BY (title,year);

all_top1_movie_occ_avg_rating = FOREACH group_movie_occ_avg_rating {
    sorted_movie_occ_avg_rating = ORDER movie_occ_avg_rating BY avg_rating DESC;
    top1_movie_occ_avg_rating = LIMIT sorted_movie_occ_avg_rating 1;
    GENERATE flatten(top1_movie_occ_avg_rating);
};

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER) ##########################

-- GROUP BY MOVIE AND USERS GENDER
group_movie_gender_rating_ = GROUP all_data BY (movie_data::movieID, ratings_with_users_data::users_no_occupation_id::raw_users::gender);
group_movie_gender_rating = FILTER group_movie_gender_rating_ BY COUNT(all_data) >= 5;

-- GET AVERAGE RATING OF MOVIE FOR EACH GENDER
movie_gender_avg_rating = FOREACH group_movie_gender_rating GENERATE FLATTEN(group) AS (movieID, gender), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_gender_avg_rating = GROUP movie_gender_avg_rating BY movieID;

-- GENDER GIVING BEST AVERAGE RATING
all_top1_movie_gender_avg_rating = FOREACH group_movie_gender_avg_rating {
    sorted_movie_gender_avg_rating = ORDER movie_gender_avg_rating BY avg_rating DESC;
    top1_movie_gender_avg_rating = LIMIT sorted_movie_gender_avg_rating 1;
    GENERATE FLATTEN(top1_movie_gender_avg_rating);
};

-- GENDER GIVING WORST REVIEWS
all_bott1_movie_gender_avg_rating = FOREACH group_movie_gender_avg_rating {
    sorted_movie_gender_avg_rating = ORDER movie_gender_avg_rating BY avg_rating ASC;
    bott1_movie_gender_avg_rating = LIMIT sorted_movie_gender_avg_rating 1;
    GENERATE FLATTEN(bott1_movie_gender_avg_rating);
};

-- CREATE RANK WITH RATING DIFFERENCE BETWEEN M & F
all_top_movie_gender_avg_rating_ = JOIN all_top1_movie_gender_avg_rating BY movieID, all_bott1_movie_gender_avg_rating BY movieID;
all_top_movie_gender_avg_rating_diff = FOREACH all_top_movie_gender_avg_rating_ GENERATE top1_movie_gender_avg_rating::movieID AS movieID, top1_movie_gender_avg_rating::gender AS gender,top1_movie_gender_avg_rating::avg_rating AS avg_rating, (top1_movie_gender_avg_rating::avg_rating - bott1_movie_gender_avg_rating::avg_rating) AS diff; 

join_all_top_movie_gender_avg_rating_diff = JOIN movie_data BY movieID, all_top_movie_gender_avg_rating_diff BY movieID;
all_top_movie_gender_avg_rating = FOREACH join_all_top_movie_gender_avg_rating_diff GENERATE movie_data::title,movie_data::year,all_top_movie_gender_avg_rating_diff::gender,all_top_movie_gender_avg_rating_diff::avg_rating, all_top_movie_gender_avg_rating_diff::diff; 

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (AGE) ##########################

-- GROUP BY MOVIE AND USERS AGE
group_movie_age_rating_ = GROUP all_data BY (movie_data::movieID, ratings_with_users_data::users_no_occupation_id::raw_users::age);
group_movie_age_rating = FILTER group_movie_age_rating_ BY COUNT(all_data) >= 5;

-- GET AVERAGE RATING OF MOVIE OVER ALL AGES
movie_age_avg_rating = FOREACH group_movie_age_rating GENERATE FLATTEN(group) AS (movieID, age), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_age_avg_rating = GROUP movie_age_avg_rating BY movieID;

all_top1_movie_age_avg_rating_ = FOREACH group_movie_age_avg_rating {
    sorted_movie_age_avg_rating = ORDER movie_age_avg_rating BY avg_rating DESC;
    top1_movie_age_avg_rating = LIMIT sorted_movie_age_avg_rating 1;
    GENERATE FLATTEN(top1_movie_age_avg_rating);
};

join_all_top1_movie_age_avg_rating_ = JOIN movie_data BY movieID, all_top1_movie_age_avg_rating_ BY movieID; 
all_top1_movie_age_avg_rating = FOREACH join_all_top1_movie_age_avg_rating_ GENERATE movie_data::title, movie_data::year, top1_movie_age_avg_rating::age, top1_movie_age_avg_rating::avg_rating;

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (OCCUPATION, GENDER) ##########################

-- GROUP BY MOVIE USERS OCCUPATION & GENDER
group_movie_occ_gender_rating_ = GROUP all_data BY (movie_data::movieID, ratings_with_users_data::users_no_occupation_id::occupation_names::name, ratings_with_users_data::users_no_occupation_id::raw_users::gender);
group_movie_occ_gender_rating = FILTER group_movie_occ_gender_rating_ BY COUNT(all_data) >= 5;

-- GET AVERAGE RATING OF MOVIE OVER ALL OCCUPATION & GENDERS
movie_occ_gender_avg_rating = FOREACH group_movie_occ_gender_rating GENERATE FLATTEN(group) AS (movieID, occupation, gender), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_occ_gender_avg_rating = GROUP movie_occ_gender_avg_rating BY movieID;

-- GET BEST RATING FOR EACH MOVIE
all_top1_movie_occ_gender_avg_rating_ = FOREACH group_movie_occ_gender_avg_rating {
    sorted_movie_occ_gender_avg_rating = ORDER movie_occ_gender_avg_rating BY avg_rating DESC;
    top1_movie_occ_gender_avg_rating = LIMIT sorted_movie_occ_gender_avg_rating 1;
    GENERATE FLATTEN(top1_movie_occ_gender_avg_rating);
};

join_all_top1_movie_occ_gender_avg_rating_ = JOIN movie_data BY movieID, all_top1_movie_occ_gender_avg_rating_ BY movieID;
all_top1_movie_occ_gender_avg_rating = FOREACH join_all_top1_movie_occ_gender_avg_rating_ GENERATE movie_data::title, movie_data::year, top1_movie_occ_gender_avg_rating::occupation,top1_movie_occ_gender_avg_rating::gender, top1_movie_occ_gender_avg_rating::avg_rating;

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER, AGE) ##########################

-- GROUP BY MOVIE USERS OCCUPATION & GENDER
group_movie_gender_age_rating_ = GROUP all_data BY (movie_data::movieID, ratings_with_users_data::users_no_occupation_id::raw_users::gender, ratings_with_users_data::users_no_occupation_id::raw_users::age);
group_movie_gender_age_rating = FILTER group_movie_gender_age_rating_ BY COUNT(all_data) >=5;
 
-- GET AVERAGE RATING OF MOVIE OVER ALL OCCUPATION & GENDERS
movie_gender_age_avg_rating = FOREACH group_movie_gender_age_rating GENERATE FLATTEN(group) AS (movieID, gender, age), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_gender_age_avg_rating = GROUP movie_gender_age_avg_rating BY movieID;

-- GET BEST RATING FOR EACH MOVIE
all_top1_movie_gender_age_avg_rating_ = FOREACH group_movie_gender_age_avg_rating {
    sorted_movie_gender_age_avg_rating = ORDER movie_gender_age_avg_rating BY avg_rating DESC;
    top1_movie_gender_age_avg_rating = LIMIT sorted_movie_gender_age_avg_rating 1;
    GENERATE FLATTEN(top1_movie_gender_age_avg_rating);
};

join_all_top1_movie_gender_age_avg_rating_ = JOIN movie_data BY movieID, all_top1_movie_gender_age_avg_rating_ BY movieID;
all_top1_movie_gender_age_avg_rating = FOREACH join_all_top1_movie_gender_age_avg_rating_ GENERATE movie_data::title, movie_data::year, top1_movie_gender_age_avg_rating::gender,top1_movie_gender_age_avg_rating::age, top1_movie_gender_age_avg_rating::avg_rating;

-- ########################## WORST MOVIES WITH BEST SCORE BY MINOR AUDIENCES ##########################

data_age_below_18 = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::raw_users::age <= 18;
group_data_age_below_18_ = GROUP data_age_below_18 BY movie_data::movieID;
group_data_age_below_18 = FILTER group_data_age_below_18_ BY COUNT(data_age_below_18) >=5;

data_age_below_18_avg_ratings = FOREACH group_data_age_below_18 GENERATE FLATTEN(group) AS movieID, AVG(data_age_below_18.rating) AS avg_rating;

data_age_above_18 = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::raw_users::age > 18;
group_data_age_above_18_ = GROUP data_age_above_18 BY movie_data::movieID;
group_data_age_above_18 = FILTER group_data_age_above_18_ BY COUNT(data_age_above_18)>=5;

data_age_above_18_avg_ratings = FOREACH group_data_age_above_18 GENERATE FLATTEN(group) AS movieID, AVG(data_age_above_18.rating) AS avg_rating;

join_data_below_above_18_ = JOIN data_age_below_18_avg_ratings BY movieID, data_age_above_18_avg_ratings BY movieID;
join_data_below_above_18 = JOIN movie_data BY movieID, join_data_below_above_18_ BY data_age_below_18_avg_ratings::movieID;

filter_join_data_below_above_18 = FILTER join_data_below_above_18 BY data_age_above_18_avg_ratings::avg_rating < data_age_below_18_avg_ratings::avg_rating;
sorted_join_data_below_above_18 = ORDER filter_join_data_below_above_18 BY data_age_above_18_avg_ratings::avg_rating ASC, data_age_below_18_avg_ratings::avg_rating DESC;
top5_data_below_above_18_ = LIMIT sorted_join_data_below_above_18 5;

top5_data_below_above_18 = FOREACH top5_data_below_above_18_ GENERATE movie_data::title AS title, movie_data::year AS year, data_age_below_18_avg_ratings::avg_rating AS rating_below_18, data_age_above_18_avg_ratings::avg_rating AS rating_above_18;

-- ########################## WORST RATED-BY-EDUCATOR MOVIES WITH BEST SCORE BY COLLEGE/GRAD STUDENTS ##########################

data_academics = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::occupation_names::name == 'academic/educator';
data_students = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::occupation_names::name == 'college/grad student';

group_data_academics_ = GROUP data_academics BY movie_data::movieID;
group_data_academics = FILTER group_data_academics_ BY COUNT(data_academics) >= 5;

group_data_students_ = GROUP data_students BY movie_data::movieID;
group_data_students = FILTER group_data_students_ BY COUNT(data_students) >= 5;

data_academics_avg_ratings = FOREACH group_data_academics GENERATE FLATTEN(group) AS movieID, AVG(data_academics.rating) AS avg_rating;
data_students_avg_ratings = FOREACH group_data_students GENERATE FLATTEN(group) AS movieID, AVG(data_students.rating) AS avg_rating;

join_data_academics_students_ = JOIN data_academics_avg_ratings BY movieID, data_students_avg_ratings BY movieID;
join_data_academics_students = JOIN movie_data BY movieID, join_data_academics_students_ BY data_academics_avg_ratings::movieID;

filter_join_data_academics_students = FILTER join_data_academics_students BY data_academics_avg_ratings::avg_rating < data_students_avg_ratings::avg_rating;
sorted_join_data_academics_students = ORDER filter_join_data_academics_students BY data_academics_avg_ratings::avg_rating ASC, data_students_avg_ratings::avg_rating DESC;
top5_data_academics_students_ = LIMIT sorted_join_data_academics_students 5;

top5_data_academics_students = FOREACH top5_data_academics_students_ GENERATE movie_data::title AS title, movie_data::year AS year, data_students_avg_ratings::avg_rating AS students_rating, data_academics_avg_ratings::avg_rating AS academics_rating;

-- ########################## PESIMISTIC AGE GROUPS (DESCENDING) ##########################

group_ratings_by_age = GROUP ratings_with_users_data BY users_no_occupation_id::raw_users::age;
ratings_by_age_avg = FOREACH group_ratings_by_age GENERATE group AS age, AVG(ratings_with_users_data.rating) AS avg_rating;
sorted_ratings_by_age_avg = ORDER ratings_by_age_avg BY avg_rating DESC;

-- ########################## YEAR OF BEST REVIEWED MOVIE FOR EACH AGE ##########################

age_movie_ratings = FOREACH all_data GENERATE ratings_with_users_data::users_no_occupation_id::raw_users::age AS age, movie_data::movieID AS movieID, raw_ratings::rating AS rating;
group_age_movie_ratings_ = GROUP age_movie_ratings BY (age, movieID);
group_age_movie_ratings = FILTER group_age_movie_ratings_ BY COUNT(age_movie_ratings) >= 5; 

age_movie_avg_ratings_ = FOREACH group_age_movie_ratings GENERATE FLATTEN(group) AS (age, movieID), AVG(age_movie_ratings.rating) AS avg_rating;

join_age_movie_avg_ratings_ = JOIN age_movie_avg_ratings_ BY movieID, movie_data BY movieID;
age_movie_avg_ratings = FOREACH join_age_movie_avg_ratings_ GENERATE age_movie_avg_ratings_::age, movie_data::title, movie_data::year, age_movie_avg_ratings_::avg_rating AS avg_rating;

group_age_movie_avg_ratings = GROUP age_movie_avg_ratings BY age;

-- GET TOP MOVIE GENRE FOR EACH OCCUPATION
all_top_age_movie_avg_ratings = FOREACH group_age_movie_avg_ratings {
    sorted_age_movie_avg_ratings = ORDER age_movie_avg_ratings BY avg_rating DESC;
    top_age_movie_avg_ratings = LIMIT sorted_age_movie_avg_ratings 1;
    GENERATE FLATTEN(top_age_movie_avg_ratings);
};

-- ########################## TEST ##########################
-- DEFINE LIMIT TO SEE FIRST 10 ROWS IN OUTPUT
-- output_limit = LIMIT top5_data_artists_students 10;
-- DUMP output_limit; 
