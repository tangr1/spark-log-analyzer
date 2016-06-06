for i in {1..7}
do
    file=
    for (( j=$(($i+1)); j<=31; j++ ))
    do
        file="$file data/zhiyu/5-$j"
    done
    spark-submit --class "com.ctofunds.dd.zhiyu.SaudiBuyRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.SaudiRetentionRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.ConversionRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.BuyRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.RetentionRate" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i $file
    #spark-submit --class "com.ctofunds.dd.zhiyu.ActiveUser" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar data/zhiyu/5-$i
    #spark-submit --class "com.ctofunds.dd.zhiyu.PageView" --master "local[4]" target/scala-2.10/dd-assembly-0.0.16.jar "data/zhiyu/1605${i}_pageview*" data/zhiyu/5-$i
done
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
