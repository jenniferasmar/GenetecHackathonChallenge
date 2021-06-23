package main.java;

import com.azure.messaging.servicebus.*;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobAccessPolicy;
import com.azure.storage.blob.models.BlobSignedIdentifier;
import com.azure.storage.blob.models.PublicAccessType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class objective extends Thread {

	static String connectionString = ""; //connection String for receiving Azure Bus message
	static String topicName = ""; //connection string for recieving license plates from Azure Bus message
	static String subName = ""; //subscription name for recieving wanted plate updates from Azure Bus message
	static List<String> wantedVehicles ; //list of wanted vehicles
	static int oldCount;
	static double balance = 100; //start balance of 100 at 5pm
	static String connectionStringWantedPlates = ""; //connection String for receiving wanted plates from Azure Bus message
	static String topicNameWantedPlates = ""; //connection string for recieving wanted plate updates from Azure Bus message
	static String subNameWantedPlates = ""; //subscription name for recieving wanted plate updates from Azure Bus message
	static String AzureStorageConnectionString = "";    //Azure Storage connection string
	static HashMap<Character,String[]> groupMap = new HashMap(); //map for fuzzy matching
	static String subscriptionKey = ""; //Subscription key for sending to Azure OCR
	static boolean foundFuzzy = false; //to determine if a fuzzy match was found while permuting a license plate being processed


	public static void main(String[] args) throws InterruptedException  {

		//initialize elements in the groupMap for fuzzyMatching
		groupMap.put('B',new String[]{"8"});
		groupMap.put('8',new String[]{"B"});
		groupMap.put('C',new String[]{"G"});
		groupMap.put('G',new String[]{"C"});
		groupMap.put('E',new String[]{"F"});
		groupMap.put('F',new String[]{"E"});
		groupMap.put('K',new String[]{"X","Y"});
		groupMap.put('X',new String[]{"K","Y"});
		groupMap.put('Y',new String[]{"K","X"});
		groupMap.put('I',new String[]{"1","T","J"});
		groupMap.put('J',new String[]{"1","T","I"});
		groupMap.put('T',new String[]{"I","1","J"});
		groupMap.put('1',new String[]{"I","T","J"});
		groupMap.put('S',new String[]{"5"});
		groupMap.put('5',new String[]{"S"});
		groupMap.put('O',new String[]{"D","Q","0"});
		groupMap.put('D',new String[]{"O","Q","0"});
		groupMap.put('Q',new String[]{"O","D","0"});
		groupMap.put('0',new String[]{"O","D","Q"});
		groupMap.put('P',new String[]{"R"});
		groupMap.put('R',new String[]{"P"});
		groupMap.put('Z',new String[]{"2"});
		groupMap.put('2',new String[]{"Z"});

		getWantedVehicles();
		System.out.println("Initial List: " + wantedVehicles.toString() );
		objective thread = new objective();
		thread.start();
	}

	//received plate readings from Service Bus 1
	static void receiveMessages() throws InterruptedException
	{
		CountDownLatch countdownLatch = new CountDownLatch(1);

		// Create an instance of the processor through the ServiceBusClientBuilder
		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
				.connectionString(connectionString)
				.processor()
				.topicName(topicName)
				.subscriptionName(subName)
				.processMessage(objective::processMessage)
				.processError(context -> processError(context, countdownLatch))
				.buildProcessorClient();

		System.out.println("Starting the processor");
		processorClient.start();

		//we will keep listening for new messages and process them to update the wanted list

		//TimeUnit.SECONDS.sleep(10);
		//System.out.println("Stopping and closing the processor");
		//processorClient.close();
	}

	//process readings for Licence plate
	private static void processMessage(ServiceBusReceivedMessageContext context) {
		ServiceBusReceivedMessage message = context.getMessage();
		JSONObject json = new JSONObject(message.getBody().toString());

		System.out.println("Processing new license plate : " + json.getString("LicensePlate"));

		if (wantedVehicles.contains(json.getString("LicensePlate"))){
			System.out.println("\n--------------------This License Plate is wanted. --------------");
			String contextImageReference = sendToAzureStorage(json);
			json.put("ContextImageReference", contextImageReference);
			json.remove("LicensePlateImageJpg");
			json.remove("ContextImageJpg");
			postWantedLicensePlate(json.toString());
		}else{
			fuzzyMatch(json);

			if(!foundFuzzy){
				sendToAzureOCR(json);
			}
			foundFuzzy = false;

		}
	}

	//process any errors during while service bus is running
	private static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
		System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
				context.getFullyQualifiedNamespace(), context.getEntityPath());

		if (!(context.getException() instanceof ServiceBusException)) {
			System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
			return;
		}

		ServiceBusException exception = (ServiceBusException) context.getException();
		ServiceBusFailureReason reason = exception.getReason();

		if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
				|| reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
				|| reason == ServiceBusFailureReason.UNAUTHORIZED) {
			System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n",
					reason, exception.getMessage());

			countdownLatch.countDown();
		} else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
			System.out.printf("Message lock lost for message: %s%n", context.getException());
		} else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
			try {
				// Choosing an arbitrary amount of time to wait until trying again.
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				System.err.println("Unable to sleep for period of time");
			}
		} else {
			System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(),
					reason, context.getException());
		}
	}

	//Make a post request to the API Plateform for a wantedLicensePlate
	static void postWantedLicensePlate(String wantedLicensePlate){
		CloseableHttpClient httpClient = HttpClients.createDefault();
		System.out.println("Posting : " + wantedLicensePlate);
		HttpPost httpPost = new HttpPost("https://licenseplatevalidator.azurewebsites.net/api/lpr/platelocation");

		StringEntity entity = null;
		String encoding = ""; //provide required encoding for post request

		try {
			entity = new StringEntity(wantedLicensePlate);
			httpPost.setEntity(entity);
			httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);
			CloseableHttpResponse response = httpClient.execute(httpPost);

			if(response.getStatusLine().getStatusCode()==200){
				System.out.println(response.getStatusLine().getStatusCode() + ": Successfully posted Wanted License Plate. Rewarding 49$ !");
				balance += 49;
				System.out.println("New balance: " + balance + " $.\n");
			}
			else{
				System.out.println(response.getStatusLine().getStatusCode() + ": " + response.getStatusLine().getReasonPhrase() + "\n");
			}

			httpClient.close();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//Get UpdatedWantedVehicles from Service bus 2
	static void getUpdateWantedVehicles() throws InterruptedException {
		CountDownLatch countdownLatch = new CountDownLatch(1);

		// Create an instance of the processor through the ServiceBusClientBuilder
		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
				.connectionString(connectionStringWantedPlates)
				.processor()
				.topicName(topicNameWantedPlates)
				.subscriptionName(subNameWantedPlates)
				.processMessage(objective::processMessageWantedPlates)
				.processError(context -> processError(context, countdownLatch))
				.buildProcessorClient();

		System.out.println("Starting to look for updates");
		processorClient.start();

//		TimeUnit.SECONDS.sleep();
//		System.out.println("Stopping and closing the processor");
//		processorClient.close();
	}

	//Process total wanted count
	private static void processMessageWantedPlates (ServiceBusReceivedMessageContext context) {
		ServiceBusReceivedMessage message = context.getMessage();
		System.out.println(message.getBody().toString());

		System.out.println("Updating Wanted List. Gonna cost you 30$.");
		System.out.println("before update: " + wantedVehicles.toString());
		getWantedVehicles();
		System.out.println("after update:  " + wantedVehicles.toString());

	}

	//Make Get request to retrieve wantedPlates
	static void getWantedVehicles(){
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpGet httpGet = new HttpGet(""); //endpoint required to make get request
		String encoding = ""; //required encoding to make request

		httpGet.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);

		try {
			CloseableHttpResponse response = httpClient.execute(httpGet);
			String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");

			//object mapper for response
			ObjectMapper mapper = new ObjectMapper();
			wantedVehicles = mapper.readValue(responseString, new TypeReference<List<String>>(){});
			oldCount = wantedVehicles.size();
			System.out.println(response.getStatusLine().getStatusCode() + ": Got " + oldCount + " wanted vehicles.");

			balance -= 30;
			System.out.println("New balance: " + balance + " $.");

			httpClient.close(); //close http connection
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static String sendToAzureStorage(JSONObject licensePlate){

		String containerName = "wantedvehicleimages";
		String encodedContextImageJpg = licensePlate.getString("ContextImageJpg");

		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(AzureStorageConnectionString).buildClient();
		BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName); //create container and return container client object

		//set permisions of blob storage so it can be read when accessing later.
		BlobSignedIdentifier identifier = new BlobSignedIdentifier()
				.setId("name")
				.setAccessPolicy(new BlobAccessPolicy()
						.setStartsOn(OffsetDateTime.now())
						.setExpiresOn(OffsetDateTime.now().plusDays(7))
						.setPermissions("r"));
		try {
			blobContainerClient.setAccessPolicy(PublicAccessType.CONTAINER, Collections.singletonList(identifier));
//			System.out.printf("Set Access Policy completed %n");
		} catch (UnsupportedOperationException error) {
//			System.out.printf("Set Access Policy completed %s%n", error);
		}


		byte[] decodedBytes = Base64.getDecoder().decode(encodedContextImageJpg);
		InputStream dataStream = new ByteArrayInputStream(decodedBytes);

		String blobName = licensePlate.getString("LicensePlate").toLowerCase() +".jpg";

		BlobClient blobClient = blobContainerClient.getBlobClient(blobName);// Get a reference to a blob
		blobClient.upload(dataStream, encodedContextImageJpg.length(),true);


		String contextImageReference = blobClient.getBlobUrl(); //url to return

//		System.out.println("\nUploading to Blob storage as blob:\n\t" + contextImageReference);
		System.out.println("Successfully uploaded to Blob Storage.\n");


		return contextImageReference;
	}

	static void fuzzyMatch(JSONObject json){
		String licensePlate = json.getString("LicensePlate");
		permute(licensePlate, 0, json);
	}

	//get permutations for fuzzy match
	static void permute(String str,  int i, JSONObject json){

		if(i==str.length()){
			return;
		}

		for(; i<str.length(); i++){
			if(groupMap.containsKey(str.charAt(i))){

				String[] group = groupMap.get(str.charAt(i));

				for(int j=0; j<group.length; j++){
					String character = group[j];
					String result = str.substring(0,i) + character + str.substring(i+1);
					if(wantedVehicles.contains(result)){
						System.out.println("\n--------------------Found a wanted Fuzzy plate:" + result +"-----------------");
						foundFuzzy = true;
						String contextImageReference = sendToAzureStorage(json);
						json.put("ContextImageReference", contextImageReference);
						json.remove("LicensePlateImageJpg");
						json.remove("ContextImageJpg");
						json.put("LicensePlate",result);
						postWantedLicensePlate(json.toString());
						return;
					}
					permute(result, i+1, json);
				}
			}
		}
	}

	//Send image to Azure OCR to recognize plate
	static void sendToAzureOCR(JSONObject json){
		String encodedLicensePlateImageJpg = json.getString("LicensePlateImageJpg");
		byte[] decodedBytes = Base64.getDecoder().decode(encodedLicensePlateImageJpg);
		String mode = "Printed";
		HttpClient httpclient = HttpClients.createDefault();

		try
		{
			URIBuilder builder = new URIBuilder("https://eastus.api.cognitive.microsoft.com/vision/v2.0/recognizeText");

			builder.setParameter("mode", mode);

			URI uri = builder.build();
			HttpPost request = new HttpPost(uri);
			request.setHeader("Content-Type", "application/octet-stream");
			request.setHeader("Ocp-Apim-Subscription-Key", subscriptionKey);

			// Byte Array Request body
			ByteArrayEntity reqEntity = new ByteArrayEntity(decodedBytes);
			request.setEntity(reqEntity);

			System.out.println("Sending License Plate to Azure OCR");
			HttpResponse response = httpclient.execute(request);
			//HttpEntity entity = response.getEntity();

			String operationLocation = response.getHeaders("Operation-Location")[0].toString(); //retrieve the url which will contain the Azure OCR results
			String[] operationLocationSplit = operationLocation.split("/");
			String operationLocationID = operationLocationSplit[operationLocationSplit.length-1];

			System.out.print("\n" + response.getStatusLine().getStatusCode());
			if(response.getStatusLine().getStatusCode() == 202){
				System.out.println(": Successfuly sent license plate image to Azure OCR! Go to:  " + operationLocation );
				getAzureRecognitionText(operationLocationID, json);
			}else{
				System.out.println(": " + response.getStatusLine().getReasonPhrase());
			}

			System.out.println("\n");
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());

		}

	}

	static void getAzureRecognitionText(String operationLocationID, JSONObject originalJSON){

		HttpClient httpclient = HttpClients.createDefault();

		try
		{
			String URIpath = "https://eastus.api.cognitive.microsoft.com/vision/v2.0/textOperations/" + operationLocationID;
			URIBuilder builder = new URIBuilder(URIpath);

			URI uri = builder.build();
			HttpGet request = new HttpGet(uri);
			request.setHeader("Ocp-Apim-Subscription-Key", subscriptionKey);

			System.out.println("Waiting on Azure ...");

			wait(4000);

			HttpResponse response = httpclient.execute(request);
			HttpEntity entity = response.getEntity();

			if (entity != null)
			{
				System.out.println(response.getStatusLine().getStatusCode() + ": Received Azure Text");
				JSONObject json = new JSONObject(EntityUtils.toString(entity));
				String statusCode = json.getString("status");

				switch (statusCode){
					case "Failed":
						System.out.println("TheText recognition process failed");
						break;
					case "Succeeded":

						JSONArray lines = json.getJSONObject("recognitionResult")
								.getJSONArray("lines");

						System.out.println("\n");
						for(int i=0; i<lines.length(); i++){
							JSONObject potentialPlate = (JSONObject) lines.get(i);
							String text =  potentialPlate.getString("text");
							text = text.replaceAll("\\s+",""); //remove any white spaces
							System.out.println("Azure found text: " + text + " testing to see if its wanted ...");
							if( (text.length()==6 || text.length()==7) && wantedVehicles.contains(text) ){

								System.out.println("\n----------------------------------------  This License Plate is wanted and detected by AZURE : -----------------------------");
								String contextImageReference = sendToAzureStorage(originalJSON);
								originalJSON.put("ContextImageReference", contextImageReference);
								originalJSON.remove("LicensePlateImageJpg");
								originalJSON.remove("ContextImageJpg");
								originalJSON.put("LicensePlate",text);
								postWantedLicensePlate(originalJSON.toString());
								return ;
							}
						}
						break;
					default:
						System.out.println("Running or Not Started. Adding to runningAzureTextRecognition list.");
				}

			}
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	//waiting time
	public static void wait(int ms)
	{
		try
		{
			Thread.sleep(ms);
		}
		catch(InterruptedException ex)
		{
			Thread.currentThread().interrupt();
		}
	}

	public void run() {
		System.out.println("This code is running in a thread");
		try {
			receiveMessages();
			getUpdateWantedVehicles();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
