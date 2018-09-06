# Movies Analytics Case Study

Just pull the repository and download files from link given below and run command in Directory containing build.sbt
 
> sbt run


# Problem Statement

https://www.kaggle.com/cesarcf1977/movielens-data-analysis-beginner-s-first/data

1. **PLOT#1**: Number of movies and ratings per year.
**INSIGHT#1**: Number of movies released per year increasing almost exponentially until 2009,
then flattening and dropping signifincantly in 2014 (2015 data is incomplete). Does this confirm expontential growth (i.e. bubbles)
is seldom sustainable in the long term? No ratings before 1995, likely to do with the availability of Internet to general public.


2. **PLOT#2**: Cumulative number of movies, in total and per genre.
**INSIGHT#2**: On average, movies are categorized into 2 genres (i.e. number of movies-genres 54k doubles the number of movies 27k).
Comedy 8.3k and Drama 13.3k are the top genres used.

3. **PLOT#3**: Distributions by genre, on top of total rating distribution. This will help identifying consitent ratings or outliers (e.g. Comedies being rated higher in general).
**INSIGHT#3**: All genres show a similar pattern (right-skewed log-normal distribution??), except perhaps Horror movies which are a bit skewed to the left (poorer ratings)...people don't like being scared, no matter how good the movie is fro a technical point of view? Movies without a tagged genre (no-genres listed) are also outliers, but likely due to the low number of ocurrences.


4. **PLOT#4**: Average rating for all individual movies.
**INSIGHT#4**: Especially after 1970, it seems there are more lower ratings, but also more higher (4.5-5.0)...it could just an effect of having more movies. Not many insights from this plot.

5. **PLOT#5**: Average rating for all movies in each year, and also per genre.
**INSIGHT#5**: Slight decline in average movie ratings after 1960, but still remains above 3. Range quite narrow, except for a few outliers.



6. **PLOT#6**: Average ratings per user.
**INSIGHT#6**: Users have a positive bias in general, with roughly 95% of their average ratings above the mid-point of 2.5. This is to be expected, and could have many explanations: users actually watch the better movies due to available ratings (and this should get better over time, as the rating system expands); users don't bother that much to rate bad movies as they do with the good ones (i.e. we don't want other to know we watched such a piece of s***), etc.
