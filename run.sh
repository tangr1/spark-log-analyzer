#for i in {1..7}
#do
#    file=
#    for (( j=$(($i+1)); j<=31; j++ ))
#    do
#        file="$file data/zhiyu/5-$j"
#    done
#    spark-submit --class "com.ctofunds.dd.zhiyu.SaudiBuyRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.SaudiRetentionRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.ConversionRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.BuyRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.RetentionRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.ActiveUser" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
    #spark-submit --class "com.ctofunds.dd.zhiyu.PageView" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar "data/zhiyu/1605${i}_pageview*" data/zhiyu/5-$i
#done
#for i in {1..31}
#do
    #spark-submit --class "com.ctofunds.dd.zhiyu.NewUser" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
    #spark-submit --class "com.ctofunds.dd.zhiyu.DetailUser" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
    #spark-submit --class "com.ctofunds.dd.zhiyu.OrderUser" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
    #spark-submit --class "com.ctofunds.dd.zhiyu.OpenTimes" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
    #spark-submit --class "com.ctofunds.dd.zhiyu.UsingTime" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
    #spark-submit --class "com.ctofunds.dd.zhiyu.GoodsCount" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
#    spark-submit --class "com.ctofunds.dd.zhiyu.QualityUser" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
#done
#file=
#for i in {1,4,7,10,13,16,19,22,24,27,30}
#do
#    file="$file data/zhiyu/5-$i"
#done
#echo $file
#spark-submit --class "com.ctofunds.dd.zhiyu.ConversionRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar $file
#for i in {1..31}
#do
#    #spark-submit --class "com.ctofunds.dd.zhiyu.ActiveUser" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar "data/zhiyu/5-${i}"
#    spark-submit --class "com.ctofunds.dd.zhiyu.NewConversionRateSaudi" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar 5-${i}
#done

for i in {22..31}
do
    index=$i
    if [ $i -lt 10 ]; then
        index=0$i
    fi
    ./submit.sh DailyRefreshArticle data/newsdog/hindi_refresh_$index,data/newsdog/hindi_media_refresh_$index $i
    #./submit.sh DailyRefreshArticle data/newsdog/hindi_refresh_$index $i
    #./submit.sh DailyRefresh data/newsdog/hindi_refresh_$index,data/newsdog/hindi_media_refresh_$index $i
    #./submit.sh DailyRefresh data/newsdog/hindi_refresh_$index $i
    #./submit.sh DailyRead data/newsdog/hindi_read_$index $i
    #./submit.sh DailyRead data/newsdog/hindi_read_$index,data/newsdog/hindi_media_read_$index $i
    #./submit.sh DailyDuration data/newsdog/hindi_duration_$index $i
    #./submit.sh DailyDuration data/newsdog/hindi_duration_$index,data/newsdog/hindi_media_duration_$index $i
    #./submit.sh DailyQualityUser ./data/newsdog/hindi_duration_$index,./data/newsdog/hindi_media_duration_$index 10 900 $i
    #./submit.sh DailyActiveUser data/newsdog/hindi_refresh_$index,data/newsdog/hindi_media_refresh_$index $i
    #./submit.sh RawRefresh "data/newsdog/hindi/*2016-07-${index}*-rec*log" data/newsdog/hindi_refresh_$index
    #./submit.sh RawRead "data/newsdog/hindi/*2016-07-${index}*-read*log" data/newsdog/hindi_read_$index
    #./submit.sh RawDuration "data/newsdog/hindi/*2016-07-${index}*-dur*log" data/newsdog/hindi_duration_$index
    #./submit.sh RawRefresh data/newsdog/hindi_media/2016-07-${index}-photo-rec.log,data/newsdog/hindi_media/video-2016-07-${index}-reclog data/newsdog/hindi_media_refresh_$index
    #./submit.sh RawRead data/newsdog/hindi_media/2016-07-${index}-photo-read.log,data/newsdog/hindi_media/video-2016-07-${index}-readlog data/newsdog/hindi_media_read_$index
    #./submit.sh RawDuration data/newsdog/hindi_media/2016-07-${index}-photo-dur.log,data/newsdog/hindi_media/video-2016-07-${index}-durlog data/newsdog/hindi_media_duration_$index
    #./submit.sh DailyQualityUser ./data/newsdog/en_duration_$index,./data/newsdog/en_media_duration_$index 10 900 $i
    #./submit.sh DailyRefreshArticle data/newsdog/en_refresh_$index,data/newsdog/en_media_refresh_$index $i
    #./submit.sh DailyRefreshArticle data/newsdog/en_refresh_$index $i
    #./submit.sh DailyRefresh data/newsdog/en_refresh_$index,data/newsdog/en_media_refresh_$index $i
    #./submit.sh DailyRefresh data/newsdog/en_refresh_$index $i
    #./submit.sh DailyRead data/newsdog/en_read_$index $i
    #./submit.sh DailyRead data/newsdog/en_read_$index,data/newsdog/en_media_read_$index $i
    #./submit.sh DailyDuration data/newsdog/en_duration_$index,data/newsdog/en_media_duration_$index $i
    #./submit.sh DailyActiveUser data/newsdog/en_refresh_$index,data/newsdog/en_media_refresh_$index $i
    #./submit.sh RawRefresh "data/newsdog/media/*2016-07-0${index}*-rec*log" data/newsdog/en_media_refresh_$index
    #./submit.sh RawRead "data/newsdog/media/*2016-07-0${index}*-read*log" data/newsdog/en_media_read_$index
    #./submit.sh RawDuration "data/newsdog/media/*2016-07-0${index}*-dur*log" data/newsdog/en_media_duration_$index
    #./submit.sh RawRefresh data/newsdog/media/2016-07-${index}-photo-rec.log,data/newsdog/media/video-2016-07-${index}-reclog data/newsdog/en_media_refresh_$index
    #./submit.sh RawRead data/newsdog/media/2016-07-${index}-photo-read.log,data/newsdog/media/video-2016-07-${index}-readlog data/newsdog/en_media_read_$index
    #./submit.sh RawDuration data/newsdog/media/2016-07-${index}-photo-dur.log,data/newsdog/media/video-2016-07-${index}-durlog data/newsdog/en_media_duration_$index
done
#file=
#for (( i=1; i<=21; i++ ))
#do
#    index=$i
#    if [ $i -lt 10 ]; then
#        index=0$i
#    fi
#    file="$file,data/newsdog/hindi_refresh_$index"
#done
#echo $file
