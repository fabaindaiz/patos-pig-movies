-- LOAD DATA
-- raw_ratings = LOAD 'hdfs://cm:9000/uhadoop2023/group14/ratings_t.dat' USING PigStorage('\t') AS (userID, movieID, rating, timestamp);
-- raw_users = LOAD 'hdfs://cm:9000/uhadoop2023/group14/users_t.dat' USING PigStorage('\t') AS (userID, gender, age, occupation, zipCode);
-- raw_movies = LOAD 'hdfs://cm:9000/uhadoop2023/group14/movies_t.dat' USING PigStorage('\t') AS (movieID, title, genres);

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
group_occ_movie = GROUP ratings_with_users_data BY (occupation_names::name, ratings_data::raw_ratings::movieID);

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

-- GET USERS OCCUPATION, MOVIE TITLE, MOVIE RATING AND MOVIE GENRES FROM ALL_DATA
occ_movie_rating_genre = FOREACH all_data GENERATE ratings_with_users_data::name AS occupation, movie_data::title AS title, ratings_with_users_data::rating AS rating, FLATTEN(movie_data::genres) AS genre;

-- GROUP BY USERS OCCUPATION AND MOVIE GENRE
group_occ_genre = GROUP occ_movie_rating_genre BY (occupation, genre);

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
group_occ_movie_genre =  GROUP occ_movie_rating_genre BY (occupation, title, genre);

-- GET THE AVERAGE RATING OF EACH MOVIE BY OCCUPATION, SEPARATED BY GENRE TOO
occ_movie_genre_avg_rating = FOREACH group_occ_movie_genre GENERATE FLATTEN(group) AS (occupation,title,genre), AVG(occ_movie_rating_genre.rating) AS avg_rating;

-- GROUP BY USERS OCCUPATION
group_occ_movie_genre_avg_rating = GROUP occ_movie_genre_avg_rating BY occupation;

all_top5_occ_movie_genre_avg_rating = FOREACH group_occ_movie_genre_avg_rating {
    sorted_occ_movie_genre_avg_rating = ORDER occ_movie_genre_avg_rating BY avg_rating DESC;
    top5_occ_movie_genre_avg_rating = LIMIT sorted_occ_movie_genre_avg_rating 5;
    GENERATE flatten(top5_occ_movie_genre_avg_rating);
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
group_movie_gender_rating = GROUP all_data BY (movie_data::title, movie_data::year, ratings_with_users_data::users_no_occupation_id::raw_users::gender);

-- GET AVERAGE RATING OF MOVIE FOR EACH GENDER
movie_gender_avg_rating = FOREACH group_movie_gender_rating GENERATE FLATTEN(group) AS (title, year, gender), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_gender_avg_rating = GROUP movie_gender_avg_rating BY (title, year);

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
all_top_movie_gender_avg_rating_ = JOIN all_top1_movie_gender_avg_rating BY (title, year), all_bott1_movie_gender_avg_rating BY (title, year);
all_top_movie_gender_avg_rating = FOREACH all_top_movie_gender_avg_rating_ GENERATE top1_movie_gender_avg_rating::title,top1_movie_gender_avg_rating::year,top1_movie_gender_avg_rating::gender,top1_movie_gender_avg_rating::avg_rating, (top1_movie_gender_avg_rating::avg_rating - bott1_movie_gender_avg_rating::avg_rating) AS diff; 

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (AGE) ##########################

-- GROUP BY MOVIE AND USERS AGE
group_movie_age_rating = GROUP all_data BY (movie_data::title, movie_data::year, ratings_with_users_data::users_no_occupation_id::raw_users::age);

-- GET AVERAGE RATING OF MOVIE OVER ALL AGES
movie_age_avg_rating = FOREACH group_movie_age_rating GENERATE FLATTEN(group) AS (title, year, age), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_age_avg_rating = GROUP movie_age_avg_rating BY (title, year);

all_top1_movie_age_avg_rating = FOREACH group_movie_age_avg_rating {
    sorted_movie_age_avg_rating = ORDER movie_age_avg_rating BY avg_rating DESC;
    top1_movie_age_avg_rating = LIMIT sorted_movie_age_avg_rating 1;
    GENERATE FLATTEN(top1_movie_age_avg_rating);
};

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (OCCUPATION, GENDER) ##########################

-- GROUP BY MOVIE USERS OCCUPATION & GENDER
group_movie_occ_gender_rating = GROUP all_data BY (movie_data::title, movie_data::year, ratings_with_users_data::users_no_occupation_id::occupation_names::name, ratings_with_users_data::users_no_occupation_id::raw_users::gender);

-- GET AVERAGE RATING OF MOVIE OVER ALL OCCUPATION & GENDERS
movie_occ_gender_avg_rating = FOREACH group_movie_occ_gender_rating GENERATE FLATTEN(group) AS (title, year, occupation, gender), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_occ_gender_avg_rating = GROUP movie_occ_gender_avg_rating BY (title,year);

-- GET BEST RATING FOR EACH MOVIE
all_top1_movie_occ_gender_avg_rating = FOREACH group_movie_occ_gender_avg_rating {
    sorted_movie_occ_gender_avg_rating = ORDER movie_occ_gender_avg_rating BY avg_rating DESC;
    top1_movie_occ_gender_avg_rating = LIMIT sorted_movie_occ_gender_avg_rating 1;
    GENERATE FLATTEN(top1_movie_occ_gender_avg_rating);
};

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER, AGE) ##########################

-- GROUP BY MOVIE USERS OCCUPATION & GENDER
group_movie_gender_age_rating = GROUP all_data BY (movie_data::title, movie_data::year, ratings_with_users_data::users_no_occupation_id::raw_users::gender, ratings_with_users_data::users_no_occupation_id::raw_users::age);

-- GET AVERAGE RATING OF MOVIE OVER ALL OCCUPATION & GENDERS
movie_gender_age_avg_rating = FOREACH group_movie_gender_age_rating GENERATE FLATTEN(group) AS (title, year, gender, age), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_gender_age_avg_rating = GROUP movie_gender_age_avg_rating BY (title, year);

-- GET BEST RATING FOR EACH MOVIE
all_top1_movie_gender_age_avg_rating = FOREACH group_movie_gender_age_avg_rating {
    sorted_movie_gender_age_avg_rating = ORDER movie_gender_age_avg_rating BY avg_rating DESC;
    top1_movie_gender_age_avg_rating = LIMIT sorted_movie_gender_age_avg_rating 1;
    GENERATE FLATTEN(top1_movie_gender_age_avg_rating);
};

-- ########################## WORST MOVIES WITH BEST SCORE BY MINOR AUDIENCES ##########################

data_age_below_18 = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::raw_users::age <= 18;
group_data_age_below_18 = GROUP data_age_below_18 BY (movie_data::title,movie_data::year);
data_age_below_18_avg_ratings = FOREACH group_data_age_below_18 GENERATE FLATTEN(group) AS (title,year), AVG(data_age_below_18.rating) AS avg_rating;

data_age_above_18 = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::raw_users::age > 18;
group_data_age_above_18 = GROUP data_age_above_18 BY (movie_data::title,movie_data::year);
data_age_above_18_avg_ratings = FOREACH group_data_age_above_18 GENERATE FLATTEN(group) AS (title,year), AVG(data_age_above_18.rating) AS avg_rating;

join_data_below_above_18 = JOIN data_age_below_18_avg_ratings BY (title,year), data_age_above_18_avg_ratings BY (title,year);

sorted_join_data_below_above_18 = ORDER join_data_below_above_18 BY data_age_below_18_avg_ratings::avg_rating DESC, data_age_above_18_avg_ratings::avg_rating ASC;
top5_data_below_above_18_ = LIMIT sorted_join_data_below_above_18 5;

top5_data_below_above_18 = FOREACH top5_data_below_above_18_ GENERATE data_age_below_18_avg_ratings::title AS title, data_age_below_18_avg_ratings::year AS year, data_age_below_18_avg_ratings::avg_rating AS rating_below_18, data_age_above_18_avg_ratings::avg_rating AS rating_above_18;

-- ########################## WORST RATED-BY-EDUCATOR MOVIES WITH BEST SCORE BY COLLEGE/GRAD STUDENTS ##########################

data_artists = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::occupation_names::name == 'academic/educator';
data_students = FILTER all_data BY ratings_with_users_data::users_no_occupation_id::occupation_names::name == 'college/grad student';

group_data_artists = GROUP data_artists BY (movie_data::title,movie_data::year);
group_data_students = GROUP data_students BY (movie_data::title,movie_data::year);

data_artists_avg_ratings = FOREACH group_data_artists GENERATE FLATTEN(group) AS (title,year), AVG(data_artists.rating) AS avg_rating;
data_students_avg_ratings = FOREACH group_data_students GENERATE FLATTEN(group) AS (title,year), AVG(data_students.rating) AS avg_rating;

join_data_artists_students = JOIN data_artists_avg_ratings BY (title,year), data_students_avg_ratings BY (title,year);

sorted_join_data_artists_students = ORDER join_data_artists_students BY data_students_avg_ratings::avg_rating DESC, data_artists_avg_ratings::avg_rating ASC;
top5_data_artists_students_ = LIMIT sorted_join_data_artists_students 5;

top5_data_artists_students = FOREACH top5_data_artists_students_ GENERATE data_students_avg_ratings::title AS title, data_students_avg_ratings::year AS year, data_students_avg_ratings::avg_rating AS students_rating, data_artists_avg_ratings::avg_rating AS artists_rating;

-- ########################## PESIMISTIC AGE GROUPS (DESCENDING) ##########################
-- ########################## YEAR OF BEST REVIEWED MOVIE FOR EACH AGE ##########################


-- ########################## TEST ##########################
-- DEFINE LIMIT TO SEE FIRST 10 ROWS IN OUTPUT
output_limit = LIMIT proportion_votes_age 10;
DUMP output_limit;

-- TAL VEZ LUEGO SEPARAR NUEVAMENTE POR OCUPACIÓN O POR EDADES
