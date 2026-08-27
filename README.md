  This repository contains a custom `TimestampExtractor` (`VBTimestampExtractor`) for the Confluent S3 Sink
Connector. It is designed to prevent Kafka Connect tasks from crashing when performing time-based partitioning and
log rotation on "Poison Pill" messages (e.g., missing or malformed timestamp fields).

  ## The Problem
  When using `TimeBasedPartitioner` with `timestamp.extractor=RecordField` alongside time-based log rotation
configurations like `partition.duration.ms` or `rotate.interval.ms`, the `S3SinkTask` directly calls the
`TimestampExtractor.extract()` method.
  If the expected date field is missing or contains stringified JSON, the native `RecordFieldTimestampExtractor`
immediately throws a `DataException`. This instantly crashes the Sink Task, halts the data pipeline, and bypasses
the Dead Letter Queue (DLQ).

  ## Architecture & Implementation Details
  We resolved the issue by wrapping the native `extract` method. When the original method fails to
extract a timestamp, instead of crashing the system, it catches the exception and safely falls back to the Kafka
record's metadata timestamp (`CreateTime`) or the current system time (`System.currentTimeMillis()`).

  * **Extended Class:**
    `io.confluent.connect.storage.partitioner.TimeBasedPartitioner.RecordFieldTimestampExtractor`
  * **Original Confluent Repo Path:**
    `kafka-connect-storage-common/partitioner/src/main/java/io/confluent/connect/storage/partitioner/TimeBasedPartitioner.java`

## Deployment
1. Build the project using Maven to generate the JAR file (or just download it from releases page):
   ```bash
   mvn clean package

2. Copy the generated JAR file into the S3 Sink Connector's plugin directory on your Kafka Connect workers.
  For example:
  cp target/custom-partitioner-1.2.0.jar /kafka/confluent-7.9.7/share/confluent-hub-components/confluentinc-kafka-connect-s3-10.5.13/

3. Restart the Kafka Connect service on all worker nodes to load the new JAR into the Classpath.

## Configuration
Update your S3 Sink Connector configuration to utilize the custom extractor.
Note: The partitioner.class must remain as the default TimeBasedPartitioner.

  {
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "timestamp.field": "RecordDate",
    "timestamp.extractor": "com.vakifbank.VBTimestampExtractor"
  }
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TR

Bu repository, Confluent S3 Sink Connector için özel olarak geliştirilmiş bir TimestampExtractor
(VBTimestampExtractor) içerir.
TimeBased klasörleme ve dosya rotasyonu yaparken, beklenen tarih alanı (RecordDate vb.) eksik 
veya bozuk geldiğinde Kafka Connect task'ının çökmesini engellemektir.

## Problem 
Connector konfigürasyonunda partition.duration.ms veya rotate.interval.ms kullanıldığında S3 Sink Task dosya
rotasyonu kararı verebilmek için doğrudan TimestampExtractor.extract() metodunu çağırır.
Eğer veri bozuksa veya aranılan tarih alanı JSON içerisinde yoksa, orijinal RecordFieldTimestampExtractor anında
DataException fırlatır. Bu durum task'ı doğrudan çökerterek veri akışını durdurur ve kaydın DLQ'ya (Dead Letter
Queue) gitmesine bile fırsat vermez.

## Çözüm
Orijinal metodun exception fırlattığı durumlarda sistemi çökertmek yerine kaydın Kafka'ya geliş saatini veya anlık sistem saatini "fallback" olarak döndürür.

Extend Edilen Sınıf:
io.confluent.connect.storage.partitioner.TimeBasedPartitioner.RecordFieldTimestampExtractor
kafka-connect-storage-common/partitioner/src/main/java/io/confluent/connect/storage/partitioner/TimeBasedPartitioner.java

## Kurulum
1. Projeyi derleyerek JAR dosyasını oluşturun (ya da direkt olarak releases sayfasından indirin):
  mvn clean package

2. Oluşan JAR dosyasını Kafka Connect sunucularınızdaki S3 Sink Connector plugin klasörüne kopyalayın.
Örneğin:
  cp target/custom-partitioner-1.2.0.jar /kafka/confluent-7.9.7/share/confluent-hub-components/confluentinc-kafka-connect-s3-10.5.13/

3. Yeni JAR dosyasının Classpath'e dahil olması için Kafka Connect servisini yeniden başlatın.
   systemctl restart kafkaconnect

## Konfigürasyon
S3 Sink Connector ayarlarınızı aşağıdaki gibi güncelleyin.
  {
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "timestamp.field": "RecordDate", #timestamp alanı
    "timestamp.extractor": "com.vakifbank.VBTimestampExtractor"
  }
