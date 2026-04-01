package com.amazon.kinesis.kafka;

import static com.amazonaws.util.ImmutableMapParameter.of;
import static java.lang.String.format;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jsoup.Jsoup;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleWithSAMLRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import net.dongliu.requests.Header;
import net.dongliu.requests.Proxies;
import net.dongliu.requests.Requests;

public class AmazonKinesisSinkTask extends SinkTask {

	private String streamName;

	private String regionName;

	private int maxConnections;

	private int rateLimit;

	private int maxBufferedTime;

	private int ttl;

	private String metricsLevel;

	private String metricsGranuality;

	private String metricsNameSpace;

	private boolean aggregration;

	private boolean usePartitionAsHashKey;

	private KinesisProducer kinesisProducer;

	private boolean samlAuthenticationEnabled;

	private Properties externalProps;

	private String profile;

	private String kinesisEndPoint;

	private String username;

	private String rolearn;

	private String intProxy;

	private String password;

	private String samlProviderName;

	private long credExpiration = Long.MAX_VALUE;

	private volatile boolean credRefreshFlag;

	final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
		@Override
		public void onFailure(Throwable t) {
			if (t instanceof UserRecordFailedException) {
				Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
				throw new DataException("Kinesis Producer was not able to publish data - " + last.getErrorCode() + "-"
						+ last.getErrorMessage());

			}
			throw new DataException("Exception during Kinesis put", t);
		}

		@Override
		public void onSuccess(UserRecordResult result) {

		}
	};

	@Override
	public String version() {
		return null;
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) {
		kinesisProducer.flush();
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		if (samlAuthenticationEnabled && !sinkRecords.isEmpty()) {
			if ((credExpiration - Calendar.getInstance().getTimeInMillis() < 300_000L) && !credRefreshFlag) {
				credRefreshFlag = true;
				while (kinesisProducer.getOutstandingRecordsCount() != 0) {
					try {
						Thread.sleep(10_000L);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new DataException("Interrupted while waiting for outstanding records before credential refresh", e);
					}
				}
				kinesisProducer.flushSync();
				kinesisProducer.destroy();
				kinesisProducer = getKinesisProducer();
				credRefreshFlag = false;
			}
			if ((credExpiration - Calendar.getInstance().getTimeInMillis() < 300_000L) && credRefreshFlag) {
				while (credRefreshFlag) {
					try {
						Thread.sleep(5_000L);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new DataException("Interrupted while waiting for credential refresh", e);
					}
				}
			}
		}

		String partitionKey;
		for (SinkRecord sinkRecord : sinkRecords) {
			ListenableFuture<UserRecordResult> f;
			if (sinkRecord.key() != null && !sinkRecord.key().toString().trim().equals("")) {
				partitionKey = sinkRecord.key().toString().trim();
			} else {
				partitionKey = Integer.toString(sinkRecord.kafkaPartition());
			}

			if (usePartitionAsHashKey)
				f = kinesisProducer.addUserRecord(streamName, partitionKey,
						Integer.toString(sinkRecord.kafkaPartition()),
						DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));
			else
				f = kinesisProducer.addUserRecord(streamName, partitionKey,
						DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));

			Futures.addCallback(f, callback, MoreExecutors.directExecutor());

		}
	}

	@Override
	public void start(Map<String, String> props) {

		streamName = props.get(AmazonKinesisSinkConnector.STREAM_NAME);

		maxConnections = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_CONNECTIONS));

		rateLimit = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RATE_LIMIT));

		maxBufferedTime = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_BUFFERED_TIME));

		ttl = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RECORD_TTL));

		regionName = props.get(AmazonKinesisSinkConnector.REGION);

		metricsLevel = props.get(AmazonKinesisSinkConnector.METRICS_LEVEL);

		metricsGranuality = props.get(AmazonKinesisSinkConnector.METRICS_GRANUALITY);

		metricsNameSpace = props.get(AmazonKinesisSinkConnector.METRICS_NAMESPACE);

		aggregration = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.AGGREGRATION_ENABLED));

		usePartitionAsHashKey = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.USE_PARTITION_AS_HASH_KEY));

		samlAuthenticationEnabled = Boolean
				.parseBoolean(props.get(AmazonKinesisSinkConnector.SAML_AUTHENTICATION_ENABLED));

		if (samlAuthenticationEnabled) {
			loadSamlConfiguration(props);
		}

		kinesisProducer = getKinesisProducer();

	}

	private void loadSamlConfiguration(Map<String, String> props) {
		String configPath = props.get(AmazonKinesisSinkConnector.EXTERNAL_CONFIG_FILE);
		profile = props.get(AmazonKinesisSinkConnector.PROFILE);
		kinesisEndPoint = props.get(AmazonKinesisSinkConnector.ENDPOINT);
		username = props.get(AmazonKinesisSinkConnector.USERNAME);
		rolearn = props.get(AmazonKinesisSinkConnector.ROLEARN);
		intProxy = props.get(AmazonKinesisSinkConnector.INT_PROXY);
		samlProviderName = props.get(AmazonKinesisSinkConnector.SAML_PROVIDER_NAME);
		if (samlProviderName == null || samlProviderName.isEmpty()) {
			samlProviderName = "SAML_ADFS3";
		}

		if (configPath == null || configPath.isEmpty()) {
			throw new ConnectException("samlAuthenticationEnabled requires " + AmazonKinesisSinkConnector.EXTERNAL_CONFIG_FILE);
		}
		if (profile == null || kinesisEndPoint == null || username == null || rolearn == null || intProxy == null) {
			throw new ConnectException(
					"SAML authentication requires profile, kinesisEndPoint, username, rolearn, intProxy, sysSeq, projSeq, credId");
		}
		String sysSeq = props.get(AmazonKinesisSinkConnector.SYS_SEQ);
		String projSeq = props.get(AmazonKinesisSinkConnector.PROJ_SEQ);
		String credId = props.get(AmazonKinesisSinkConnector.CRED_ID);
		if (sysSeq == null || projSeq == null || credId == null) {
			throw new ConnectException("SAML authentication requires sysSeq, projSeq, and credId");
		}

		externalProps = new Properties();
		try (InputStream input = new FileInputStream(configPath)) {
			externalProps.load(input);
		} catch (IOException e) {
			throw new ConnectException("Failed to load external config file: " + configPath, e);
		}

		String decryptionUrl = externalProps.getProperty("decryption.url");
		if (decryptionUrl == null || decryptionUrl.isEmpty()) {
			throw new ConnectException("external config must define decryption.url");
		}

		EncryptionUtility encryptUtil = new EncryptionUtility();
		KmsRequestDto kmsDto = new KmsRequestDto();
		kmsDto.setSystemSeq(Integer.parseInt(sysSeq.trim()));
		kmsDto.setProjectId(Integer.parseInt(projSeq.trim()));
		kmsDto.setCredentialId(Integer.parseInt(credId.trim()));

		try {
			password = encryptUtil.invokeDecryptionServices(kmsDto, decryptionUrl);
		} catch (Exception e) {
			throw new ConnectException("Password decryption failed", e);
		}
		if (password == null || "failed".equalsIgnoreCase(password)) {
			throw new ConnectException("Password decryption service did not return a valid password");
		}
	}

	@Override
	public void stop() {
		kinesisProducer.destroy();

	}

	private KinesisProducer getKinesisProducer() {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(regionName);
		if (samlAuthenticationEnabled) {
			config.setCredentialsProvider(getSamlCredential());
			config.setKinesisEndpoint(kinesisEndPoint);
			config.setLogLevel("error");
		} else {
			config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
		}
		config.setMaxConnections(maxConnections);

		config.setAggregationEnabled(aggregration);

		config.setRateLimit(rateLimit);

		config.setRecordMaxBufferedTime(maxBufferedTime);

		config.setRecordTtl(ttl);

		config.setMetricsLevel(metricsLevel);

		config.setMetricsGranularity(metricsGranuality);

		config.setMetricsNamespace(metricsNameSpace);

		return new KinesisProducer(config);

	}

	private AWSStaticCredentialsProvider getSamlCredential() {
		String[] domainUser = username.split("--");
		if (domainUser.length != 2) {
			throw new ConnectException("username must be in the form DOMAIN--USER (two segments separated by --)");
		}
		String domain = domainUser[0];
		String user = domainUser[1];
		String[] proxyParts = intProxy.split(":");
		if (proxyParts.length != 2) {
			throw new ConnectException("intProxy must be host:port");
		}
		String proxyHost = proxyParts[0];
		int proxyPort = Integer.parseInt(proxyParts[1].trim());

		String awsProxyHost = externalProps.getProperty("aws.proxy.host");
		String awsProxyPortStr = externalProps.getProperty("aws.proxy.port");
		String awsAuthUrl = externalProps.getProperty("aws.auth.url");
		if (awsProxyHost == null || awsProxyPortStr == null || awsAuthUrl == null) {
			throw new ConnectException("external config must define aws.proxy.host, aws.proxy.port, and aws.auth.url");
		}
		int awsProxyPort = Integer.parseInt(awsProxyPortStr.trim());

		Proxy proxy = Proxies.httpProxy(awsProxyHost, awsProxyPort);

		ClientConfiguration clientConf = new ClientConfiguration().withProxyHost(proxyHost).withProxyPort(proxyPort)
				.withProxyUsername(user).withProxyDomain(domain).withProxyPassword(password);

		System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
		String res = Requests.session().post(awsAuthUrl).headers(Header.of("Accept-Language", "en")).proxy(proxy)
				.body(of("UserName", format("%s\\%s", domain, user), "Password", password, "AuthMethod",
						"FormsAuthentication"))
				.send().readToText();

		String samlRes = Jsoup.parse(res).getElementsByAttributeValue("name", "SAMLResponse").get(0).attributes()
				.get("value");

		String principalArn = "arn:aws:iam::" + profile + ":saml-provider/" + samlProviderName;
		AssumeRoleWithSAMLRequest samlReq = new AssumeRoleWithSAMLRequest().withPrincipalArn(principalArn)
				.withRoleArn(rolearn).withSAMLAssertion(samlRes).withDurationSeconds(3600);

		Credentials creds = AWSSecurityTokenServiceClientBuilder.standard().withClientConfiguration(clientConf)
				.withRegion(regionName)
				.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))).build()
				.assumeRoleWithSAML(samlReq).getCredentials();

		BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(creds.getAccessKeyId(),
				creds.getSecretAccessKey(), creds.getSessionToken());

		credExpiration = creds.getExpiration().getTime();

		return new AWSStaticCredentialsProvider(sessionCredentials);
	}

}
