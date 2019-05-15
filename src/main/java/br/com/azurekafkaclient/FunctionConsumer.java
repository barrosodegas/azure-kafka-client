package br.com.azurekafkaclient;

import java.util.*;
import com.microsoft.azure.functions.annotation.*;

import br.com.barroso.kafka.avroclient.client.consumer.RunConsumerConfirmDeliveryClient;
import br.com.barroso.kafka.avroclient.client.consumer.RunConsumerInvoiceClient;
import br.com.barroso.kafka.avroclient.client.consumer.RunConsumerOrderClient;
import br.com.barroso.kafka.avroclient.client.consumer.RunConsumerPaymentClient;
import br.com.barroso.kafka.avroclient.client.consumer.RunConsumerStockSeparationClient;

import com.microsoft.azure.functions.*;

/**
 * Azure Functions with HTTP Trigger.
 */
public class FunctionConsumer {
	
    /**
     * This function listens at endpoint "/api/HttpTrigger-Java". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpTrigger-Java&code={your function key}
     * 2. curl "{your host}/api/HttpTrigger-Java?name=HTTP%20Query&code={your function key}"
     * Function Key is not needed when running locally, it is used to invoke function deployed to Azure.
     * More details: https://aka.ms/functions_authorization_keys
     */
    @FunctionName("kafka-consumer")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.FUNCTION) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        // Parse query parameter
        String query = request.getQueryParameters().get("name");
        String name = request.getBody().orElse(query);

        if (name == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("\n\nPlease pass a name on the query string or in the request body!\n").build();
        } else {
        	
        	try {
				RunConsumerOrderClient.main(null);
				RunConsumerPaymentClient.main(null);
				RunConsumerInvoiceClient.main(null);
				RunConsumerStockSeparationClient.main(null);
				RunConsumerConfirmDeliveryClient.main(null);
			} catch (Exception e) {
				e.printStackTrace();
	            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("\n\nConsumer error: " + e.getMessage() + "\n").build();
			}
            return request.createResponseBuilder(HttpStatus.OK).body("\nHi, " + name + "!\n\nExecutou os consumers!!!\n").build();
        }
    }
}
