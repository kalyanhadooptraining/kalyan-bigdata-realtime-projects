package com.orienit.kalyan.hadoop.training.mongodb;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

/**
 * MongoDB Basic Examples
 */
public class MongoDbOperations {
	public static void main(String[] args) {

		/**** Connect to MongoDB ****/
		// Since 2.10.0, uses MongoClient
		MongoClient mongo = new MongoClient("localhost", 27017);

		/**** Get database ****/
		// if database doesn't exists, MongoDB will create it for you
		DB db = mongo.getDB("kalyan");

		/**** Get collection / table from 'kalyan' ****/
		// if collection doesn't exists, MongoDB will create it for you
		DBCollection table = db.getCollection("input");

		// drop the table
		table.drop();

		/**** Insert ****/
		// create a document to store key and value
		List<BasicDBObject> list = new ArrayList<BasicDBObject>();
		list.add(0, new BasicDBObject("record", "I am going"));
		list.add(1, new BasicDBObject("record", "to hyd"));
		list.add(2, new BasicDBObject("record", "I am learning"));
		list.add(3, new BasicDBObject("record", "hadoop course"));
		table.insert(list);
		
		/*
		BasicDBObject document = new BasicDBObject();
		document.put("record", "I am going");
		table.insert(document);

		document = new BasicDBObject();
		document.put("record", "to hyd");
		table.insert(document);

		document = new BasicDBObject();
		document.put("record", "I am learning");
		table.insert(document);

		document = new BasicDBObject();
		document.put("record", "hadoop course");
		table.insert(document);
		*/

		/**** Find and display ****/
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("record", "am");

		DBCursor cursor = table.find(searchQuery);
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
		cursor.close();

		/**** Done ****/
		System.out.println("Done");

	}
}