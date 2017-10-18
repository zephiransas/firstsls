package com.serverless;

import java.text.SimpleDateFormat;
import java.util.*;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Handler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

	private static final Logger LOG = Logger.getLogger(Handler.class);

	@Override
	public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
		final String PARAM_NAME = "queryStringParameters";

		LOG.info("received: " + input);

		if(!input.containsKey(PARAM_NAME)) {
			LOG.error("query param is required!");
			return ApiGatewayResponse.builder()
					.setStatusCode(500)
					.setObjectBody("query param is required!")
					.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
					.build();
		}

		Map<String, Object> params = (Map<String, Object>) input.get(PARAM_NAME);
		writeDynamoDB(params);

		Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);
		return ApiGatewayResponse.builder()
				.setStatusCode(200)
				.setObjectBody(responseBody)
				.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
				.build();
	}

	private void writeDynamoDB(Map<String, Object> input) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder
				.standard()
				.withRegion(Regions.AP_NORTHEAST_1)
				.build();

		DynamoDB dynamoDB = new DynamoDB(client);
		Table table = dynamoDB.getTable(System.getenv("DYNAMODB_TABLE"));

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
		Item item = new Item()
				.withPrimaryKey("id", UUID.randomUUID().toString())
				.with("created_at", format.format(new Date()));
		input.forEach((key, value) -> item.with(key, value));

		table.putItem(item);
	}
}
