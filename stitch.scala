import com.twitter.util.{Await,Future, Return, JavaTimer, Throw, Try}
import com.twitter.stitch.{MapGroup, SeqGroup, Stitch}
import com.twitter.conversions.time._
import scala.util.Random

case class User(name: String)

case class Tweet(id: Long, text:Option[String], ownerId: Long) 

object UserHydrator{
  def getUsers(ids: Seq[Long]): Future[Seq[User]] = {
    println(s"Hydrating Users $ids")
    Future.value(
      ids.map{ id => User(s"user $id")}
    )
  }
}

object TweetHydrator{
  def getTweets(ids: Set[Long], viewerId: Long): Future[Map[Long,Try[Tweet]]] = {
    println(s"Hydrating Tweets $ids for viewer id: $viewerId")
    Future.value(
      ids.toSeq.map{ id => 
        val ownerId = id
        val text = if(ownerId == viewerId) {
          Some(s"tweet text $id")
        } else {
          None
        }
        (id, Return(Tweet(id, text, ownerId)))
      }.toMap
    )
  }
}

def getTextAndOwner(tweetIds: Seq[Long], viewerId: Long): Future[Seq[Try[(Option[String],User)]]] = { 
  TweetHydrator.getTweets(tweetIds.toSet, viewerId).flatMap{ tweetsMap =>
    val userIds = tweetsMap.flatMap{ case (tweetId, tweetTry) =>
      tweetTry.toOption.map(_.ownerId)
    }.toSeq
    UserHydrator.getUsers(userIds).map{ users =>
      val userMap = userIds.zip(users).toMap  
      tweetIds.map{ tweetId => 
        val tweetTry = tweetsMap(tweetId)
        tweetTry.map{ tweet => 
          val user = userMap(tweet.ownerId)
          (tweet.text, user)
        }
      }
    }
  }
}

Await.result(getTextAndOwner(Seq(1, 2, 3, 4), 3))
  
object UserStitch {
  private[this] val userGroup = new SeqGroup[Long, User] {
    override def run(keys: Seq[Long]): Future[Seq[User]] = {
      UserHydrator.getUsers(keys)
    }
  }
  def getUser(id: Long): Stitch[User] = Stitch.call(id, userGroup)
}

object TweetStitch {
  case class TweetGroup(viewerId: Long) extends MapGroup[Long, Tweet] {
    override def run(keys: Seq[Long]): Future[Long => Try[Tweet]] = {
      TweetHydrator.getTweets(keys.toSet, viewerId)
    }
  }
  def getTweet(id: Long, viewerId: Long): Stitch[Tweet] = Stitch.call(id, TweetGroup(viewerId))
}

def getTextAndOwnerStitch(tweetIds: Seq[Long], viewerId: Long): Future[Seq[Try[(Option[String],User)]]] = {
  Stitch.run{
    Stitch.traverse(tweetIds){ tweetId =>
      val stitch = for {
        tweet <- TweetStitch.getTweet(tweetId, viewerId)
        user <- UserStitch.getUser(tweet.ownerId)
      } yield (tweet.text, user)
      stitch.liftToTry
    }
  }
}

Await.result(getTextAndOwnerStitch(Seq(1, 2, 3, 4), 3))

object TweetHydratorWithErrors{
  def getTweets(ids: Set[Long], viewerId: Long): Future[Map[Long,Try[Tweet]]] = {
    println(s"Hydrating Tweets $ids for viewer id: $viewerId")
    Future.value(
      ids.toSeq.map{ id => 
        val ownerId = id
        val text = if(ownerId == viewerId) {
          Some(s"tweet text $id")
        } else {
          None
        }
        // Now we sometimes throw an exception
        val resultTry = if(Random.nextDouble < 0.7) {
          Return(Tweet(id, text, ownerId))
        } else {
          Throw(new Exception("Tweet hydration failed"))
        }
        (id, resultTry)
      }.toMap
    )
  }
}

object TweetStitchWithRetries {
  case class TweetGroup(viewerId: Long) extends MapGroup[Long, Tweet] {
    override def run(keys: Seq[Long]): Future[Long => Try[Tweet]] = {
      TweetHydratorWithErrors.getTweets(keys.toSet, viewerId)
    }
  }
  def getTweet(id: Long, viewerId: Long): Stitch[Tweet] = Stitch.call(id, TweetGroup(viewerId))
  def getTweetWithRetries(id: Long, viewerId: Long): Stitch[Tweet] = {
    getTweet(id, viewerId).rescue { case e => 
      getTweetWithRetries(id, viewerId)
    }
  }
}

def getTextAndOwnerStitchWithFailures(tweetIds: Seq[Long], viewerId: Long): Future[Seq[Try[(Option[String],User)]]] = {
  Stitch.run{
    Stitch.traverse(tweetIds){ tweetId =>
      val stitch = for {
        tweet <- TweetStitchWithRetries.getTweet(tweetId, viewerId)
        user <- UserStitch.getUser(tweet.ownerId)
      } yield (tweet.text, user)
      stitch.liftToTry
    }
  }
}

Await.result(getTextAndOwnerStitchWithFailures(Seq(1, 2, 3, 4), 3))

def getTextAndOwnerStitchWithRetries(tweetIds: Seq[Long], viewerId: Long): Future[Seq[Try[(Option[String],User)]]] = {
  Stitch.run{
    Stitch.traverse(tweetIds){ tweetId =>
      val stitch = for {
        tweet <- TweetStitchWithRetries.getTweetWithRetries(tweetId, viewerId)
        user <- UserStitch.getUser(tweet.ownerId)
      } yield (tweet.text, user)
      stitch.liftToTry
    }
  }
}

Await.result(getTextAndOwnerStitchWithRetries(Seq(1, 2, 3, 4), 3))

object BatchLogger{
  val timer = new JavaTimer(false, None)
  // logging is expensive. It takes 5 seconds.
  def logTextAndUserNames(tweetTextAndUserNames: Seq[(Option[String], String)]): Future[Seq[Unit]] = {
    timer.doLater(5.seconds){
      println(s"Logging $tweetTextAndUserNames")
      tweetTextAndUserNames.map{ _ => ()}
    }
  }
}
object LoggerStitch {
  private[this] val loggerGroup = new SeqGroup[(Option[String], String), Unit] {
    override def run(keys: Seq[(Option[String], String)]): Future[Seq[Unit]] = {
      BatchLogger.logTextAndUserNames(keys)
    }
  }
  def logTextAndUserName(text: Option[String], userName: String): Stitch[Unit] = Stitch.call((text, userName), loggerGroup)
}

def getTextAndOwnerStitchWithLogging(tweetIds: Seq[Long], viewerId: Long): Future[Seq[Try[(Option[String],User)]]] = {
  Stitch.run{
    Stitch.traverse(tweetIds){ tweetId =>
      val stitch = for {
        tweet <- TweetStitch.getTweet(tweetId, viewerId)
        user <- UserStitch.getUser(tweet.ownerId)
        // chain the logging to the end of the computation
        unit <- LoggerStitch.logTextAndUserName(tweet.text, user.name)
      } yield (tweet.text, user)
      stitch.liftToTry
    }
  }
}

def getTextAndOwnerStitchWithAsyncLoggingBroken(tweetIds: Seq[Long], viewerId: Long): Future[Seq[Try[(Option[String],User)]]] = {
  Stitch.run{
    Stitch.traverse(tweetIds){ tweetId =>
      val stitch = for {
        tweet <- TweetStitch.getTweet(tweetId, viewerId)
        user <- UserStitch.getUser(tweet.ownerId)
      } yield {
        // Don't for logging to return. Kick it off here.
        LoggerStitch.logTextAndUserName(tweet.text, user.name)
        (tweet.text, user)
      }
      stitch.liftToTry
    }
  }
}
Await.result(getTextAndOwnerStitchWithAsyncLoggingBroken(Seq(1, 2, 3, 4), 3))


def getTextAndOwnerStitchWithAsyncLogging(tweetIds: Seq[Long], viewerId: Long): Future[Seq[Try[(Option[String],User)]]] = {
  Stitch.run{
    Stitch.traverse(tweetIds){ tweetId =>
      val stitch = for {
        tweet <- TweetStitch.getTweet(tweetId, viewerId)
        user <- UserStitch.getUser(tweet.ownerId)
      } yield {
       (tweet, user)
      }
      // chain the effect on to the stich but don't change the result.
      stitch.applyEffect{ case (tweet, user) =>
        // Create a stitch that kicks of the computation, but returns immediately
        Stitch.async(LoggerStitch.logTextAndUserName(tweet.text, user.name))
      }.map{ case (tweet, user) =>
        (tweet.text, user)
      }.liftToTry
    }
  }
}

Await.result(getTextAndOwnerStitchWithAsyncLogging(Seq(1, 2, 3, 4), 3))
