package domain.count

import java.time.Duration
import java.util.Properties
import java.net.URL

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}


object TopDomains extends App {

    import org.apache.kafka.streams.scala.serialization.Serdes._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    val input_topic: String = "streams-plaintext-input"
    val output_topic: String = "streams-domain-count-output"

    val config: Properties = {
        val p = new Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "domain_count-scala-application")
        val bootstrapServers = "localhost:29092,localhost:39092,localhost:49092"
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        p
    }

    val builder = new StreamsBuilder()
    val URLs: KStream[String, String] = builder.stream[String, String](input_topic)
    val domainsCounts: KTable[String, Long] = URLs
        .mapValues(url => URL(url).getHost.split("\\.").last)
        .groupBy((_, domain) => domain)
        .count()
    domainsCounts.toStream.to(output_topic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

    streams.cleanUp()

    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
    }

}