package com.amazon.kinesis.kafka;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class EncryptionUtility {

	public String invokeDecryptionServices(KmsRequestDto kmsDto, String decryptionUrl) throws Exception {

		String str = "{ \"projectId\":\"" + kmsDto.getProjectId() + "\" ,";
		str = str + "\"systemSeq\":\"" + kmsDto.getSystemSeq() + "\",";
		str = str + " \"credentialId\":\"" + kmsDto.getCredentialId() + "\" }";

		String res = invokeRest(str, decryptionUrl);
		JSONObject jsonResponse = (JSONObject) new JSONParser().parse(res);
		String status = (String) jsonResponse.get("status");
		if (status != null && status.equalsIgnoreCase("success")) {
			return (String) jsonResponse.get("message");
		}
		return "failed";
	}

	public String invokeRest(final String json, final String url) throws Exception {
		try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
			HttpPost postRequest = new HttpPost(url);
			postRequest.setHeader("Content-Type", "application/json");
			StringEntity input = new StringEntity(json);
			postRequest.setEntity(input);
			HttpResponse response = httpClient.execute(postRequest);
			String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new Exception("Error" + responseString);
			}
			return responseString;
		}
	}
}
