from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': 300
}

with DAG(
    dag_id='traffic_analytics_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_args,
    tags=['kafka', 'spark', 'hive', 'traffic']
) as dag:

# Infra Health Check

infra_check = BashOperator(
        task_id='infra_health_check',
        bash_command="""
        jps | grep -E 'NameNode|ResourceManager|Kafka|QuorumPeerMain'
        """
    )

# Kafka Topic Check

kafka_topic_check = BashOperator(
        task_id='kafka_topic_check',
        bash_command="""
        kafka-topics.sh --list --zookeeper localhost:2181 | grep mytopic_traffic_events
        """
    )

# 1. Generate Kafka Traffic Events

produce_events = BashOperator(
        task_id='generate_kafka_events',
        bash_command="""
        source /home/kokila/traffic_venv/bin/activate &&
        python /home/kokila/Hadoop_Folder/traffic_project/kafka/producer.py
        """)

# 2. Check Spark Streaming is Running

streaming_health_check = BashOperator(
        task_id='spark_streaming_health_check',
        bash_command="jps | grep -i Spark")

# 3. Spark Structured Streaming Job (Trigger / Restart if needed)

spark_streaming = BashOperator(
        task_id='spark_streaming_job',
        bash_command="""
        spark-submit \
        --master yarn \
        --deploy-mode client \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 \
        /home/kokila/Hadoop_Folder/traffic_project/spark/traffic_streaming.py
        """,
        execution_timeout=timedelta(minutes=5))

# 4. Spark Batch Cleaning Job

spark_batch_clean = BashOperator(
        task_id='spark_batch_cleaning',
        bash_command="""
        spark-submit \
        --master yarn \
        /home/kokila/Hadoop_Folder/traffic_project/spark/traffic_batch_clean.py
        """)

# 5. Spark Batch Aggregation Job

spark_batch_agg = BashOperator(
        task_id='spark_batch_aggregation',
        bash_command="""
        spark-submit \
        --master yarn \
        /home/kokila/Hadoop_Folder/traffic_project/spark/traffic_batch_agg.py
        """)

# 6. Hive Aggregated Table Refresh

hive_refresh = BashOperator(
        task_id='refresh_hive_tables',
        bash_command="""
        hive -e "MSCK REPAIR TABLE road_traffic_analytics"
        """)

# 7. Data Quality Check

dq_check = BashOperator(
        task_id='data_quality_check',
        bash_command="""
        hive -e "
        SELECT COUNT(*) FROM traffic_clean_tb
        WHERE Speed_kmh < 0 OR Speed_kmh > 200;
        " | grep -q '^0$'
        """
    )

# DAG Dependencies
infra_check >> kafka_topic_check >> produce_events >> streaming_health_check >> spark_streaming >> spark_batch_clean >> spark_batch_agg >> hive_refresh >> dq_check