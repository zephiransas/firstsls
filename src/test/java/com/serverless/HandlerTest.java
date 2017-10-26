package com.serverless;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HandlerTest {

    private Handler handler;

    final String TABLE = "hoge";

    public HandlerTest() throws InterruptedException {
        handler = new Handler();
        handler.setTableName(TABLE);

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder
                .standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                "http://localhost:18000",
                                "ap-northeast-1")
                )
                .build();
        handler.setClient(client);

        refreshTable(client);
    }

    private void refreshTable(AmazonDynamoDB client) {
        DynamoDB dynamoDB = new DynamoDB(client);

        TableUtils.deleteTableIfExists(client, new DeleteTableRequest().withTableName(TABLE));

        List<KeySchemaElement> keySchemaElements = Arrays.asList(
                new KeySchemaElement("id", KeyType.HASH)
        );
        List<AttributeDefinition> attributeDefinitions = Arrays.asList(
                new AttributeDefinition()
                        .withAttributeName("id")
                        .withAttributeType(ScalarAttributeType.S)
        );

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(TABLE)
                .withKeySchema(keySchemaElements)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(
                        new ProvisionedThroughput()
                                .withReadCapacityUnits(1L)
                                .withWriteCapacityUnits(1L));

        Table dynamoDBTable = dynamoDB.createTable(createTableRequest);
        try {
            dynamoDBTable.waitForActive();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void test() {
        Map<String, Object> queryParams = new HashMap<String, Object>() {
            {put("key1", "value1");}
            {put("key2", "value2");}
        };
        Map<String, Object> params = new HashMap<String, Object>() {
            {put("queryStringParameters", queryParams);}
        };

        ApiGatewayResponse response = handler.handleRequest(params, null);

        assertEquals(200, response.getStatusCode());
    }
}
