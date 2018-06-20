package io.confluent.kafkarest;

import com.mapr.web.security.SslConfig;
import com.mapr.web.security.WebSecurityManager;

public class KafkaRestSSLPropertiesCLI {
  public static void main(String[] args) throws Exception{
    System.exit(new KafkaRestSSLPropertiesCLI().run(args));
  }

  public synchronized int run(String[] args) throws Exception {
    if(args.length != 1 && args[0].isEmpty() ){
      System.err.println("Usage: io.confluent.kafkarest.KafkaRestSSLPropertiesCLI " +
          "[keystoreFile | keystorePass | serverKeyPass ]");
      return 1;
    }
    try (SslConfig sslConfig = WebSecurityManager.getSslConfig()) {
      switch (args [0]) {
        case "keystoreFile":
          System.out.println(sslConfig.getServerKeystoreLocation());
          break;
        case "keystorePass":
          System.out.println(sslConfig.getServerKeystorePassword());
          break;
        case "truststoreFile":
          System.out.println(sslConfig.getServerTruststoreLocation());
          break;
        case "truststorePassword":
          System.out.println(sslConfig.getServerTruststorePassword());
          break;
        case "serverKeyPassword":
          System.out.println(sslConfig.getServerKeyPassword());
          break;
        default:
          System.err.println("Unknown option.");
          return 1;
      }
    }
    return 0;
  }

}
