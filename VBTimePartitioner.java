package com.vakifbank;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.kafka.connect.connector.ConnectRecord;

public class VBTimestampExtractor extends TimeBasedPartitioner.RecordFieldTimestampExtractor {

	@Override
	public Long extract(ConnectRecord<?> record) {
		try {
			// 1. Orijinal davranış: RecordDate içindeki saati bulmaya çalışır.
			return super.extract(record);
		} catch (Exception e) {
			// 2. Bulamazsa veya veri bozuksa, sistemi çökertmek yerine
			// sessizce Kafka'nın kendi timestamp'ini (long türünde) döndürür.
			return record.timestamp() != null ? record.timestamp() : System.currentTimeMillis();
		}
	}
}
