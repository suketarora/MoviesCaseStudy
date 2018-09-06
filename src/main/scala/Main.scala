import org.apache.spark._
import scala.math.BigDecimal
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.Set



object Main extends App  {


    case class movie  (movie_Id:Int,title:String,genres:Array[String],Year:String) extends Serializable {}

  def parseMovie(row:String):movie={
  val field = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
  val movie_Id=field(0).toInt
  val title =  field(1)
   val genres=field(2).split('|')
   val str = field(1).trim
    val length = str.length()
    var str2 = str.substring(length - 5,length).trim
    if(str.charAt(length-1) == '"') str2 = str.substring(length - 6,length-1).trim
    val Year = str2.substring(0, str2.length() - 1)
   
   movie(movie_Id,title,genres,Year)
}


case class Ratings(userId:Int,movieId:Int,rating:Double,timestamp:Long,RatingYear:String) extends Serializable{}

  
   override def main(arg: Array[String]): Unit = {
  
   
   var sparkConf = new SparkConf().setMaster("local").setAppName("MoviesAnalytics")
   var sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
  

     sc.setLogLevel("ERROR")
     
val MoviesRddRaw = sc.textFile("file:///home/suket/case_studies/MoviesCaseStudy/src/main/resources/movies.csv").filter(x => !x.contains("movieId,title,genres")) 

 import spark.implicits._
  var MoviesData = MoviesRddRaw.map(parseMovie).toDS
  var Allgenres  =MoviesData.map {movie => movie.genres}.collect.flatten.toSet.toList

  val customSchemaForRatings = StructType(Array(
         StructField("userId", IntegerType, true),
        StructField("movieId", IntegerType, true),
        StructField("rating", DoubleType, true),
        StructField("timestamp", LongType, true)))

    // # File location and type
val file_location2 = "file:///home/suket/case_studies/MoviesCaseStudy/src/main/resources/ratings.csv"
val file_type = "csv"

// # CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","


// # The applied options are for CSV files. For other file types, these will be ignored.
val ratings = spark.read.format(file_type) 
  .option("inferSchema", infer_schema) 
  .option("header", first_row_is_header) 
  .option("sep", delimiter) 
  .schema(customSchemaForRatings)
  .load(file_location2)  
val ratingsWithYear = ratings.withColumn("RatingYear",year(from_unixtime($"timestamp")).cast(StringType)).as[Ratings]
 
val moviesPerYear = MoviesData.groupBy($"Year").count().orderBy($"count".desc).collect.map { row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long] )}
// moviesPerYear: Array[(String, Long)] = Array((2009,1113), (2012,1021), (2011,1014), (2013,1010), (2008,978), (2010,958), (2007,900), (2006,854), (2014,740), (2005,739), (2004,706), (2002,678), (2003,655), (2001,632), (2000,613), (1998,554), (1999,542), (1997,528), (1996,509), (1995,474), (1994,432), (1993,371), (1992,335), (1988,325), (1990,314), (1987,313), (1991,312), (1989,310), (1986,265), (1985,254), (1981,248), (1980,243), (1982,238), (1984,234), (1983,222), (1972,219), (1973,210), (1971,204), (1970,204), (1968,203), (1979,202), (1966,199), (1976,198), (1977,197), (1975,196), (1974,195), (1978,192), (1969,177), (1964,173), (1967,173), (1965,165), (1957,163), (1962,151), (1959,151), (1963,148), (1960,148), (1958,146), (1955,146), (1953,136), (1956,136), (1952,131), (1949,126), (195...

val ratingsPerYear = ratingsWithYear.groupBy($"RatingYear").count().orderBy($"count".desc).collect.map { row => (row(0).asInstanceOf[String],row(1).asInstanceOf[Long] )}
// ratingsPerYear: Array[org.apache.spark.sql.Row] = Array([1995,4], [1996,1611737], [1997,701717], [1998,307948], [1999,1197147], [2000,1954036], [2001,1185216], [2002,870812], [2003,1036389], [2004,1169719], [2005,1802430], [2006,1172574], [2007,1053053], [2008,1159430], [2009,929572], [2010,904375], [2011,766061], [2012,731176], [2013,599681], [2014,563013], [2015,284173])

var GenreWiseMovies : Array[(String,org.apache.spark.sql.Dataset[movie])] = Array.ofDim(Allgenres.length)

 for ( count <- 0 to Allgenres.length-1){

  GenreWiseMovies(count) = (Allgenres(count), MoviesData.filter(movie => movie.genres.contains(Allgenres(count))))

}

// GenreWiseMovies : Array[(String, org.apache.spark.sql.Dataset[movie])] = Array((Animation,[movie_Id: int, title: string ... 2 more fields]), (Thriller,[movie_Id: int, title: string ... 2 more fields]), (War,[movie_Id: int, title: string ... 2 more fields]), (Horror,[movie_Id: int, title: string ... 2 more fields]), (Documentary,[movie_Id: int, title: string ... 2 more fields]), (Comedy,[movie_Id: int, title: string ... 2 more fields]), (Western,[movie_Id: int, title: string ... 2 more fields]), (Fantasy,[movie_Id: int, title: string ... 2 more fields]), (Romance,[movie_Id: int, title: string ... 2 more fields]), (Drama,[movie_Id: int, title: string ... 2 more fields]), (IMAX,[movie_Id: int, title: string ... 2 more fields]), (Film-Noir,[movie_Id: int, title: string ... 2 more fields]), (Mystery,[m...

val MoviesCountPerGenre = GenreWiseMovies.map{ case (genre,movies) => (genre,movies.count)}.sortWith( _._2 > _._2)
// MoviesCountPerGenre : Array[(String, Long)] = Array((Drama,13344), (Comedy,8374), (Thriller,4178), (Romance,4127), (Action,3520), (Crime,2939), (Horror,2611), (Documentary,2471), (Adventure,2329), (Sci-Fi,1743), (Mystery,1514), (Fantasy,1412), (War,1194), (Children,1139), (Musical,1036), (Animation,1027), (Western,676), (Film-Noir,330), ((no genres listed),246), (IMAX,196))


var GenreWiseMoviesWithRatings :  Array[(String, org.apache.spark.sql.DataFrame)] = Array.ofDim(Allgenres.length)

 for ( count <- 0 to Allgenres.length-1){

  GenreWiseMoviesWithRatings(count) = ( GenreWiseMovies(count)._1, GenreWiseMovies(count)._2.join(ratingsWithYear,$"movie_Id" === $"movieId") )


}

var GenreWiseRatings :  Array[(String, Array[Double])] = Array.ofDim(Allgenres.length)

 for ( count <- 0 to Allgenres.length-1){

  GenreWiseRatings(count) = ( GenreWiseMoviesWithRatings(count)._1, GenreWiseMoviesWithRatings(count)._2.agg(avg("rating")).collect.map { row => (BigDecimal(row(0).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)})


}

// GenreWiseRatings: Array[(String, Array[Double])] = Array((Animation,Array(3.62)), (Thriller,Array(3.51)), (War,Array(3.81)), (Horror,Array(3.28)), (Documentary,Array(3.74)), (Comedy,Array(3.43)), (Western,Array(3.57)), (Fantasy,Array(3.51)), (Romance,Array(3.54)), (Drama,Array(3.67)), (IMAX,Array(3.66)), (Film-Noir,Array(3.97)), (Mystery,Array(3.66)), (Musical,Array(3.56)), ((no genres listed),Array(3.01)), (Sci-Fi,Array(3.44)), (Adventure,Array(3.5)), (Children,Array(3.41)), (Crime,Array(3.67)), (Action,Array(3.44)))

var AverageRatingsOfGenreYearWise : Array[(String, Array[(String, Double)])] = Array.ofDim(Allgenres.length)
for ( count <- 0 to Allgenres.length-1){

  AverageRatingsOfGenreYearWise(count) = (GenreWiseMoviesWithRatings(count)._1,GenreWiseMoviesWithRatings(count)._2.groupBy("RatingYear").avg("rating").orderBy("RatingYear").collect.map { row => (row(0).asInstanceOf[String],BigDecimal(row(1).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)})

}
// AverageRatingsOfGenreYearWise: Array[(String, Array[(String, Double)])] = Array((Animation,Array((1996,3.69), (1997,3.65), (1998,3.58), (1999,3.65), (2000,3.68), (2001,3.64), (2002,3.6), (2003,3.6), (2004,3.54), (2005,3.54), (2006,3.55), (2007,3.52), (2008,3.59), (2009,3.61), (2010,3.65), (2011,3.65), (2012,3.67), (2013,3.71), (2014,3.66), (2015,3.58))), (Thriller,Array((1995,4.0), (1996,3.6), (1997,3.53), (1998,3.45), (1999,3.56), (2000,3.51), (2001,3.49), (2002,3.47), (2003,3.44), (2004,3.39), (2005,3.4), (2006,3.43), (2007,3.47), (2008,3.54), (2009,3.49), (2010,3.52), (2011,3.56), (2012,3.62), (2013,3.65), (2014,3.61), (2015,3.5))), (War,Array((1996,3.96), (1997,3.96), (1998,3.9), (1999,3.96), (2000,3.93), (2001,3.85), (2002,3.78), (2003,3.74), (2004,3.69), (2005,3.68), (2006,3.7), (...

val AverageRatingsOfIndividualMovies = MoviesData.join(ratingsWithYear,$"movie_Id" === $"movieId").groupBy("movieId").agg(avg("rating")).collect.map { row => (row(0).asInstanceOf[Int],BigDecimal(row(1).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) }
// AverageRatingsOfIndividualMovies: Array[(Int, Double)] = Array((148,2.89), (463,2.8), (471,3.66), (496,3.29), (833,2.73), (1088,3.21), (1238,3.97), (1342,2.95), (1580,3.56), (1591,2.62), (1645,3.48), (1829,3.09), (1959,3.63), (2122,2.6), (2142,2.99), (2366,3.55), (2659,3.23), (2866,3.61), (3175,3.6), (3749,3.26), (3794,3.27), (3918,2.92), (3997,2.07), (4101,3.3), (4519,3.25), (4818,2.47), (4900,3.11), (4935,3.49), (5156,2.91), (5300,3.64), (5518,3.45), (5803,2.77), (6336,3.1), (6357,3.67), (6397,3.54), (6466,3.58), (6620,3.81), (6654,2.97), (6658,2.88), (7240,2.66), (7253,3.24), (7340,2.93), (7754,2.0), (7833,3.86), (7880,3.28), (7982,3.63), (7993,3.24), (8592,3.25), (8638,3.94), (26425,3.8), (26708,4.0), (26755,2.23), (27484,3.17), (27760,3.35), (30970,3.39), (31035,3.72), (31367,2.88)...

val AverageRatingsPerUser =  ratingsWithYear.groupBy("userId").avg("rating").orderBy("userId").collect.map { row => (row(0).asInstanceOf[Int],BigDecimal(row(1).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) }
// AverageRatingsPerUser: Array[(Int, Double)] = Array((1,3.74), (2,4.0), (3,4.12), (4,3.57), (5,4.27), (6,3.75), (7,3.29), (8,3.8), (9,3.06), (10,3.89), (11,3.95), (12,3.53), (13,3.73), (14,3.75), (15,3.2), (16,3.52), (17,4.04), (18,3.61), (19,3.84), (20,3.43), (21,3.71), (22,3.59), (23,3.94), (24,3.33), (25,3.62), (26,3.49), (27,3.85), (28,2.81), (29,3.49), (30,2.89), (31,3.38), (32,3.45), (33,3.3), (34,3.86), (35,4.25), (36,2.8), (37,3.4), (38,3.49), (39,3.7), (40,3.33), (41,4.12), (42,4.03), (43,3.66), (44,4.27), (45,3.09), (46,3.41), (47,2.45), (48,3.92), (49,3.8), (50,4.15), (51,3.58), (52,3.35), (53,4.1), (54,3.44), (55,4.17), (56,3.48), (57,4.1), (58,4.13), (59,3.4), (60,4.07), (61,3.34), (62,4.39), (63,4.2), (64,4.34), (65,3.8), (66,3.83), (67,4.06), (68,3.5), (69,3.14), (70,2.76)...

val AverageRatingsOfIndividualMoviesYearWise = MoviesData.join(ratingsWithYear,$"movie_Id" === $"movieId").groupBy("movieId","RatingYear").avg("rating").orderBy("movieId").collect.map { row => (row(0).asInstanceOf[Int],row(1).asInstanceOf[String],BigDecimal(row(2).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) }
 // AverageRatingsOfIndividualMoviesYearWise: Array[(Int, String, Double)] = Array((1,1999,3.97), (1,1997,3.88), (1,1996,4.13), (1,2009,3.77), (1,2008,3.74), (1,2001,4.12), (1,1998,3.88), (1,2015,3.86), (1,2000,4.14), (1,2006,3.68), (1,2013,3.95), (1,2011,3.86), (1,2005,3.76), (1,2010,3.86), (1,2014,3.95), (1,2003,4.0), (1,2002,4.05), (1,2012,3.93), (1,2007,3.67), (1,2004,3.86), (2,2005,2.87), (2,1996,3.56), (2,2000,3.15), (2,2004,2.89), (2,1998,3.26), (2,2010,3.16), (2,2009,3.04), (2,2001,3.14), (2,2006,2.87), (2,2002,3.08), (2,1999,3.14), (2,2003,2.96), (2,2008,3.06), (2,1997,3.46), (2,2007,2.92), (2,2012,3.2), (2,2014,3.34), (2,2013,3.27), (2,2011,3.18), (2,2015,3.13), (3,2000,3.03), (3,1997,3.29), (3,2002,3.02), (3,1996,3.41), (3,2005,2.68), (3,2013,3.06), (3,2008,3.11), (3,2001,3.2), (...


     sc.stop()

   }

}

