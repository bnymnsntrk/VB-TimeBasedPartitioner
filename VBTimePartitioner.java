package com.vakifbank;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class VBTimePartitioner<T> extends TimeBasedPartitioner<T> {

	private DateTimeFormatter fallbackFormatter;

	@Override
	public void configure(Map<String, Object> config) {
		super.configure(config);

		// Connector config'indeki format ve lokasyon bilgilerini alıyoruz
		String pathFormat = (String) config.getOrDefault("path.format", "YYYY/MM/dd/HHmmss");
		String timeZoneId = (String) config.getOrDefault("timezone", "Europe/Istanbul");

		// Java 8 Time standardı "YYYY" yerine "yyyy" kullandığı için ufak bir koruma:
		String javaFormat = pathFormat.replace("YYYY", "yyyy");

		this.fallbackFormatter = DateTimeFormatter.ofPattern(javaFormat)
												  .withZone(ZoneId.of(timeZoneId));
	}

	@Override
	public String encodePartition(SinkRecord sinkRecord) {
		try {
			// 1. Orijinal Partitioner çalıştırılır. (Eğer RecordDate varsa ve düzgünse, her zamanki gibi
klasörler)
			return super.encodePartition(sinkRecord);
		} catch (Exception e) {
			// 2. EĞER RECORDDATE YOKSA VEYA BOZUKSA BURAYA DÜŞER!
			// Sistemin çökmesini engelleyip hatayı yutuyoruz.

			// Kaydın Kafka'ya geliş saatini (CreateTime) alıyoruz, yoksa anlık saati alıyoruz:
			long fallbackTs = (sinkRecord.timestamp() != null) ? sinkRecord.timestamp() : System.
currentTimeMillis();

			// 3. Klasör ismini biz kendimiz (fallback timestamp ile) manuel oluşturup döndürüyoruz!
			return fallbackFormatter.format(Instant.ofEpochMilli(fallbackTs));
		}
	}
}
