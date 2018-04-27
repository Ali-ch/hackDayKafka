package com.zengularity.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.http.HttpMethod.POST;

public class ApiConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ApiConsumer.class);

  public static List<Map> consumeApi() throws IOException {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Afkl-Travel-Country", "FR");
    headers.set("Accept", "application/hal+json;profile=com.afklm.b2c.flightoffers.available-offers.v1;charset=utf8");
    headers.set("Afkl-Travel-Host", "AF");
    headers.set("Accept-Language", "en-US");
    headers.set("Api-Key", "3j83edfz3nexhv3d86xydeqt");
    headers.set("Content-Type", "application/json");


    List<String> list = Files.readAllLines(Paths.get("jsonString"));
    String concat = list.stream().collect(Collectors.joining());


    RestTemplate restTemplate = new RestTemplate();
    ResponseEntity<String> response = restTemplate.exchange("https://api.klm.com/opendata/flightoffers/available-offers",
        POST,
        new HttpEntity<>(concat, headers),
        String.class);

    return parseJsonResponse(response.getBody());
  }

  private static List<Map> parseJsonResponse(String body) {

    Map<String, String> resultMap = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = null;

    try {
      rootNode = objectMapper.readTree(body);
    } catch (IOException e) {
      e.printStackTrace();
    }

    JsonNode phoneNosNode = rootNode.path("flightProducts");
    Iterator<JsonNode> elements = phoneNosNode.elements();
    List<Map> listEventVols = new ArrayList<>();

    while (elements.hasNext()) {
      JsonNode flight = elements.next();
      resultMap.put("price", flight.get("price").get("totalPrice").toString());
      JsonNode connectionNode = flight.get("connections").elements().next();
      resultMap.put("availableSeats", connectionNode.get("numberOfSeatsAvailable").toString());
      JsonNode segmentNode = connectionNode.get("segments").elements().next();
      resultMap.put("origin", segmentNode.get("origin").get("city").get("name").textValue());
      resultMap.put("destination", segmentNode.get("destination").get("city").get("name").textValue());
      resultMap.put("departureDateTime", segmentNode.get("departureDateTime").textValue());
      resultMap.put("arrivalDateTime", segmentNode.get("arrivalDateTime").textValue());
      listEventVols.add(resultMap);
    }

    return listEventVols;
  }

}