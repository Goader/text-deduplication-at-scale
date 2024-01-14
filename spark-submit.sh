./spark-3.3.4-bin-hadoop3-scala2.13/bin/spark-submit \
        --class ua.nlp.ukrlm.Main \
        --master "local[*]" \
        --executor-memory 510g \
        --driver-memory 510g \
        --conf "spark.local.dir=$SCRATCH/haltiukczyjt/spark" \
        --conf "spark.eventLog.enabled=true" \
        --conf "spark.eventLog.dir=$SCRATCH/haltiukczyjt/spark-logs" \
        --conf "spark.sql.shuffle.partitions=500" \
        --conf "spark.memory.fraction=0.98" \
        --conf "spark.memory.storageFraction=0.05" \
        jars/text-deduplication-at-scale_2.13-0.1.0-SNAPSHOT.jar \
        $SCRATCH/haltiukczyjt/cc100/uk.txt \
        $SCRATCH/haltiukczyjt/cc100/deduplicated.txt
