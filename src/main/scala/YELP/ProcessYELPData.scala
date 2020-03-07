package YELP

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ProcessYELPData {


  case class Business(
                       business_id: String
                       , name: String
                       , neighborhood: String
                       , address: String
                       , city: String
                       , state: String
                       , latitude: Double
                       , longitude: Double
                       , stars: Double
                       , review_count: Long
                       , is_open: Boolean
                       , attributes: Array[String]
                       , categories: Array[String]
                       , hours: Array[String])


  case class Review(
                     review_id: String,
                     user_id: String,
                     business_id: String,
                     stars: Double,
                     date: String,
                     text: String,
                     useful: Long,
                     funny: Long,
                     cool: Long,
                   )


  case class User(
                   user_id: String
                   , name: String
                   , review_count: Long
                   , yelping_since: String
                   , friends: Array[String]
                   , useful: Long
                   , funny: Long
                   , cool: Long
                   , fans: Long
                   , elite: Array[String]
                   , average_stars: Double
                   , compliment_hot: Long
                   , compliment_more: Long
                   , compliment_profile: Long
                   , compliment_cute: Long
                   , compliment_list: Long
                   , compliment_note: Long
                   , compliment_plain: Long
                   , compliment_cool: Long
                   , compliment_funny: Long
                   , compliment_writer: Long
                   , compliment_photos: Long

                 )


  case class CheckIn(
                      time: Array[String],
                      business_id: String,
                    )


  case class Tip(
                  text: String
                  , date: String
                  , likes: Long
                  , business_id: String
                  , user_id: String
                )

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val sparkSession: SparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext


    import sparkSession.implicits._
    val expectedBusiness = sparkSession.read.json("C:\\Venkat_DO\\yelp-data\\processor\\src\\main\\resources\\data\\yelp_academic_dataset_business.json").as[Business]
    val expectedReview = sparkSession.read.json("C:\\Venkat_DO\\yelp-data\\processor\\src\\main\\resources\\data\\yelp_academic_dataset_review.json").as[Review]
    val expectedUser = sparkSession.read.json("C:\\Venkat_DO\\yelp-data\\processor\\src\\main\\resources\\data\\yelp_academic_dataset_user.json").as[User]
    val expectedTip = sparkSession.read.json("C:\\Venkat_DO\\yelp-data\\processor\\src\\main\\resources\\data\\yelp_academic_dataset_tip.json").as[Tip]
    val expectedCheckIn = sparkSession.read.json("C:\\Venkat_DO\\yelp-data\\processor\\src\\main\\resources\\data\\yelp_academic_dataset_checkin.json").as[CheckIn]


    expectedBusiness.createOrReplaceTempView("business")
    expectedReview.createOrReplaceTempView("review")
    /*expectedUser.createOrReplaceTempView("")
    expectedTip.createOrReplaceTempView("")
    expectedCheckIn.createOrReplaceTempView("")*/

    println("------------- Number of DOCS from the Business ------------")
    println(expectedBusiness.count())
    println("-------------------------")


    println("------------- Number of DOCS from the REVIEWS ------------")
    println(expectedReview.count())
    println("-------------------------")

    sparkSession.sql(
      """
    select
        business_id,
        name business_name,
        bad_thing_count,
        review_id review_happened_at,
        date
    from(
        select
            review_id,
            business_id,
            name,
            date,
            bad_thing_count,
            case
                when
                     stars<=3 and
                     bad_thing_count>=3
                 then 1
                 else 0
                 end
                 as bad_thing_happened
        from(
            select
                review_id,
                business_id,
                name,
                stars,
                date,
                sum(bad_thing_lasting) over
                         (PARTITION BY business_id ORDER BY date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) bad_thing_count
            from(
                select
                    r.review_id,
                    b.business_id,
                    b.name,
                    r.stars,
                    r.date,
                    case
                        when
                            lag(r.stars) over (PARTITION BY b.business_id ORDER BY r.date)<=3
                        then 1
                        else 0
                        end
                        as bad_thing_lasting
                from business b
                    inner join review r on r.business_id = b.business_id
            ) T
        )
    )
    where bad_thing_happened=1
    order by date
"""
    ).show(50, false)


    /*sparkSession.sql(
      """
select /*+ MAPJOIN(stop_words) */
    business_id,
    business_name,
    word,
    avg,
    total
from(
    select
        b.business_id business_id,
        b.name business_name,
        regexp_replace(lw.w,"[^a-z]","") word,
        avg(r.stars) avg,
        sum(r.stars) total
    from review r
         inner join business b on b.business_id = r.business_id
     lateral view explode(split(lower(r.text)," ")) lw as w
    group by b.business_id,b.name,word
) T
left join stop_words sw on sw.value=T.word
where sw.value is null and T.word <> '' and total>200
order by T.avg desc,T.total desc
"""
    ).show(50, false)*/

  }
}
