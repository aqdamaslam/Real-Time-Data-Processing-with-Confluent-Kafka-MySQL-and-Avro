### **Case Study: Real-Time Data Processing with Confluent Kafka, MySQL, and Avro**

#### **Background**
The e-commerce company "BuyOnline" requires a robust real-time data pipeline to synchronize its frequently updated product information stored in a MySQL database with a downstream analytics system. The system is designed to handle incremental data fetches, real-time data serialization, and partitioning to optimize performance and scalability. Confluent Kafka, Apache Avro, and Python are used to implement this solution.

---

### **Objective**
To build a real-time streaming application that:
1. Fetches incremental data from the MySQL database.
2. Serializes the data into Avro format.
3. Publishes serialized data to a multi-partition Kafka topic.
4. Processes the data using a consumer group.
5. Writes transformed data into JSON files for downstream analytics.

---
 
### **Solution Overview**

#### **System Architecture**
1. **Data Source**: A MySQL database table (`product`) storing product information.
2. **Producer**: A Python Kafka producer fetches updated records based on the `last_updated` timestamp, serializes the data into Avro, and publishes it to a Kafka topic.
3. **Topic Configuration**: A Kafka topic named `product_updates` with 10 partitions ensures scalability and proper load distribution.
4. **Consumer Group**: A group of 5 Python Kafka consumers deserializes, transforms, and writes data into JSON files.

   
![image](https://github.com/user-attachments/assets/397afdcf-8591-41b9-9922-c37d3dfe7c16)

---

### **Implementation Steps**

#### **Step 1: MySQL Table Setup**
- A `product` table is created with the following schema:
  ```sql
  CREATE TABLE product (
      id INT PRIMARY KEY,
      name VARCHAR(255),
      category VARCHAR(255),
      price FLOAT,
      last_updated TIMESTAMP
  );
  ```

#### **Step 2: Kafka Producer**
- **Data Fetching**: The producer queries the `product` table for records where `last_updated` > `last_read_timestamp`. 
  ```sql
  SELECT id, name, category, price, last_updated 
  FROM product 
  WHERE last_updated > {last_read_timestamp};
  ```
- **Serialization**: Data is serialized into Avro format using an Avro schema.
  ```json
  {
    "type": "record",
    "name": "Product",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "category", "type": "string"},
      {"name": "price", "type": "float"},
      {"name": "last_updated", "type": "string"}
    ]
  }
  ```
- **Partitioning**: The product ID is used as the key to ensure updates for the same product are routed to the same partition.

#### **Step 3: Kafka Consumer Group**
- A consumer group of 5 consumers is set up to read messages from the `product_updates` topic. Each consumer processes messages from a subset of partitions.
- **Deserialization**: Avro data is deserialized back into Python objects.
- **Transformation Logic**:
  - Convert the `category` field to uppercase.
  - Apply business logic to update the price (e.g., apply a 10% discount for products in a specific category).

#### **Step 4: Writing to JSON Files**
- Transformed records are converted to JSON strings and appended to separate JSON files, one per consumer. Each record is written on a new line to ensure readability.

---
