package com.ctofunds.dd

import org.apache.spark.{SparkConf, SparkContext}

object RequestPath {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("musically-dau")
    val sc = new SparkContext(sparkConf)
    HadoopConfiguration.configure(args, sc.hadoopConfiguration)
    val requestPathFile = args(2)

    val rawResult = sc.textFile(requestPathFile)
      .map(line => line.split(",")(1) + "/" + line.split(",")(2))
      .cache


    val userCreated = rawResult
      .filter(_.matches("/rest/v2/users/register/POST"))
      .count

    val userFollowed = rawResult
      .filter(_.matches("/rest/v2/users/follow/.*/POST"))
      .count

    val userUnfollowed = rawResult
      .filter(_.matches("/rest/v2/users/unfollow/.*/POST"))
      .count

    val musicalReleased = rawResult
      .filter(_.matches("/rest/v2/musicals/.*/uploaded/PUT"))
      .count

    val musicalDeleted = rawResult
      .filter(_.matches("/rest/v2/musicals/.*/DELETE"))
      .count

    val musicalLiked = rawResult
      .filter(_.matches("/rest/v2/musicals/.*/like/PUT"))
      .count

    val musicalUnliked = rawResult
      .filter(_.matches("/rest/v2/musicals/.*/like/DELETE"))
      .count

    val commentCreated = rawResult
      .filter(_.matches("/rest/v2/comments/POST"))
      .count

    val commentDeleted = rawResult
      .filter(_.matches("/rest/v2/comments/.*/DELETE"))
      .count

    val commentLiked = rawResult
      .filter(_.matches("/rest/v2/comments/.*/like/PUT"))
      .count

    val commentUnliked = rawResult
      .filter(_.matches("/rest/v2/comments/.*/like/DELETE"))
      .count

    println(s"$commentCreated | $commentDeleted | $commentLiked | $commentUnliked | $musicalDeleted | $musicalLiked |" +
      s" $musicalReleased | $musicalUnliked | $userCreated | $userFollowed | $userUnfollowed")

    sc.stop()
  }
}