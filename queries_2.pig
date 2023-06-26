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

-- JOIN RATINGS AND USERS WITH OCCUPATION NAMES INSTEAD OF ID
ratings_with_users_data = JOIN raw_ratings BY userID, users_no_occupation_id BY userID;


-- ################################# TOP 10 MOVIES FOR EACH OCCUPATION ################################

-- GROUP BY OCCUPATION AND MOVIE ID
group_occ_movie = GROUP ratings_with_users_data BY (occupation_names::name, raw_ratings::movieID);

-- GET AVERAGE RATING OF MOVIE FOR EACH OCCUPATION
occ_movie_avg_rating = FOREACH group_occ_movie GENERATE FLATTEN(group) AS (occupation, movieID), AVG(ratings_with_users_data.rating) AS avg_rating;

-- JOIN OCCUPATION_MOVIE_AVG_RATING AND MOVIES
occ_movie_avg_rating_with_movies_data = JOIN occ_movie_avg_rating BY movieID, movie_data BY movieID;

-- ADD MOVIE TITLE TO RELATION AND DELETE MOVIE ID
occ_movie_title_avg_rating = FOREACH occ_movie_avg_rating_with_movies_data GENERATE occ_movie_avg_rating::occupation AS occupation, movie_data::title AS title, occ_movie_avg_rating::avg_rating AS avg_rating;

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
all_data = JOIN ratings_with_users_data BY raw_ratings::movieID, movie_data BY movieID;

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

movie_occ_avg_rating = FOREACH occ_movie_title_avg_rating GENERATE title AS title, occupation AS occupation, avg_rating AS avg_rating;
group_movie_occ_avg_rating = GROUP movie_occ_avg_rating BY title;

all_top1_movie_occ_avg_rating = FOREACH group_movie_occ_avg_rating {
    sorted_movie_occ_avg_rating = ORDER movie_occ_avg_rating BY avg_rating DESC;
    top1_movie_occ_avg_rating = LIMIT sorted_movie_occ_avg_rating 1;
    GENERATE flatten(top1_movie_occ_avg_rating);
};

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER) ##########################

-- GROUP BY MOVIE AND USERS GENDER
group_movie_gender_rating = GROUP all_data BY (movie_data::title, ratings_with_users_data::users_no_occupation_id::raw_users::gender);

-- GET AVERAGE RATING OF MOVIE FOR EACH GENDER
movie_gender_avg_rating = FOREACH group_movie_gender_rating GENERATE FLATTEN(group) AS (title, gender), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_gender_avg_rating = GROUP movie_gender_avg_rating BY title;

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
all_top_movie_gender_avg_rating_ = JOIN all_top1_movie_gender_avg_rating BY title, all_bott1_movie_gender_avg_rating BY title;
all_top_movie_gender_avg_rating = FOREACH all_top_movie_gender_avg_rating_ GENERATE top1_movie_gender_avg_rating::title,top1_movie_gender_avg_rating::gender,top1_movie_gender_avg_rating::avg_rating, (top1_movie_gender_avg_rating::avg_rating - bott1_movie_gender_avg_rating::avg_rating) AS diff; 

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (AGE) ##########################

-- GROUP BY MOVIE AND USERS AGE
group_movie_age_rating = GROUP all_data BY (movie_data::title, ratings_with_users_data::users_no_occupation_id::raw_users::age);

-- GET AVERAGE RATING OF MOVIE OVER ALL AGES
movie_age_avg_rating = FOREACH group_movie_age_rating GENERATE FLATTEN(group) AS (title, age), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_age_avg_rating = GROUP movie_age_avg_rating BY title;

all_top1_movie_age_avg_rating = FOREACH group_movie_age_avg_rating {
    sorted_movie_age_avg_rating = ORDER movie_age_avg_rating BY avg_rating DESC;
    top1_movie_age_avg_rating = LIMIT sorted_movie_age_avg_rating 1;
    GENERATE FLATTEN(top1_movie_age_avg_rating);
};

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (OCCUPATION, GENDER) ##########################

-- GROUP BY MOVIE USERS OCCUPATION & GENDER
group_movie_occ_gender_rating = GROUP all_data BY (movie_data::title, ratings_with_users_data::users_no_occupation_id::occupation_names::name, ratings_with_users_data::users_no_occupation_id::raw_users::gender);

-- GET AVERAGE RATING OF MOVIE OVER ALL OCCUPATION & GENDERS
movie_occ_gender_avg_rating = FOREACH group_movie_occ_gender_rating GENERATE FLATTEN(group) AS (title, occupation, gender), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_occ_gender_avg_rating = GROUP movie_occ_gender_avg_rating BY title;

-- GET BEST RATING FOR EACH MOVIE
all_top1_movie_occ_gender_avg_rating = FOREACH group_movie_occ_gender_avg_rating {
    sorted_movie_occ_gender_avg_rating = ORDER movie_occ_gender_avg_rating BY avg_rating DESC;
    top1_movie_occ_gender_avg_rating = LIMIT sorted_movie_occ_gender_avg_rating 1;
    GENERATE FLATTEN(top1_movie_occ_gender_avg_rating);
};

-- ########################## BEST AUDIENCE SCORE FOR EACH MOVIE (GENDER, AGE) ##########################

-- GROUP BY MOVIE USERS OCCUPATION & GENDER
group_movie_gender_age_rating = GROUP all_data BY (movie_data::title, ratings_with_users_data::users_no_occupation_id::raw_users::gender, ratings_with_users_data::users_no_occupation_id::raw_users::age);

-- GET AVERAGE RATING OF MOVIE OVER ALL OCCUPATION & GENDERS
movie_gender_age_avg_rating = FOREACH group_movie_gender_age_rating GENERATE FLATTEN(group) AS (title, gender, age), AVG(all_data.rating) AS avg_rating;

-- GROUP BY TITLE
group_movie_gender_age_avg_rating = GROUP movie_gender_age_avg_rating BY title;

-- GET BEST RATING FOR EACH MOVIE
all_top1_movie_gender_age_avg_rating = FOREACH group_movie_gender_age_avg_rating {
    sorted_movie_gender_age_avg_rating = ORDER movie_gender_age_avg_rating BY avg_rating DESC;
    top1_movie_gender_age_avg_rating = LIMIT sorted_movie_gender_age_avg_rating 1;
    GENERATE FLATTEN(top1_movie_gender_age_avg_rating);
};

-- ########################## TEST ##########################
-- DEFINE LIMIT TO SEE FIRST 10 ROWS IN OUTPUT
output_limit = LIMIT all_top1_movie_gender_age_avg_rating 10;
DUMP output_limit;

-- VER GUSTOS SEGUN ANHO
-- VER CUÁLES PELÍCULAS FUERON MÁS VECES CALIFICADAS
-- TAL VEZ LUEGO SEPARAR NUEVAMENTE POR OCUPACIÓN O POR EDADES
