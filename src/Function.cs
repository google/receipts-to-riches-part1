/*
Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using CloudNative.CloudEvents;

using Google.Api;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
    
using Google.Apis.Storage.v1.Data;
using Google.Cloud.BigQuery.V2;
using Google.Cloud.Datastore.V1;
using Google.Cloud.DocumentAI.V1Beta3;
using Google.Cloud.Functions.Framework;
using Google.Cloud.Logging.V2;
using Google.Cloud.Logging.Type;
using Google.Cloud.Storage.V1;
using Google.Events.Protobuf.Cloud.Storage.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using DsEntity =  Google.Cloud.Datastore.V1.Entity;

using Grpc.Core;

using Newtonsoft.Json;

namespace HelloGcs;


public class Invoice
{
    public int Items_sold_per_invoice { get; set; }
    public DateTime Purchase_Date { get; set; }
    public float Sales_Tax { get; set; }
    public float SubTotal { get; set; }
    public string InvoiceID { get; set; }
}


public class Item
{
    public string Item_code { get; set; }
    public float Item_cost { get; set; }
}

public class DBObject
{
    public Invoice InvoiceObj { get; set; }
    public List<Item> ItemsObj { get; set; }
}


public class Function : ICloudEventFunction<StorageObjectData>
{
    private readonly StorageClient _storageClient;
    private readonly DocumentProcessorServiceClient _documentAI;
    public static string projectId = "<--your project ID -->";
    public static string gcp_Region = "<-- GCP Region. Example us -->";
    public static string documentAI_processor = "<-- Document AI processor ID-->";
    public static string logId = "<-- Custom Log ID. For example, CF_SupermarketXBillParser -->";
    public static string outputstorage = "<--Storage bucket to where output is saved --> ";

    public Function()
    {
        _storageClient = StorageClient.Create();
        _documentAI = DocumentProcessorServiceClient.Create();
    }


    public async Task HandleAsync(CloudEvent cloudEvent, StorageObjectData data, CancellationToken cancellationToken)
    {

        var bucketName = data.Bucket;
        var objectName = data.Name;
        var uri = new Uri($"gs://{bucketName}/{objectName}");

        var document = await ProcessDocumentAsync(uri, cancellationToken);

        try{
            // Parse document obj and store it to Storage for reference
            ParseDocumentObjToStorageForRef(document);
        }catch(Exception e)
        {
            Console.WriteLine($"Error writing to storage. Here are error details {e.ToString()}");
            LogError(e);
        }

        var tempDBObject = ConvertDocumentToCustomObjects(document);
        
        try{
            InsertDBObjectToBQ(tempDBObject);
        }catch(Exception e)
        {
            Console.WriteLine($"Error writing to BQ. Here are error details {e.ToString()}");
            LogError(e);
        }

        try{
            InsertEntitiesToDatastore(tempDBObject);
        }catch(Exception e)
        {
            Console.WriteLine($"Error writing to DataStore. Here are error details {e.ToString()}");
            LogError(e);
        }

        //return Task.CompletedTask;
        return;
    }

    private static void ParseDocumentObjToStorageForRef(Document document)
    {

        string output = "";
        foreach (var entity in document.Entities)
        {
            output += "Type: " + entity.Type + "\n";
            output += "Mention Text: " + entity.MentionText + "\n";
            output += "Mention ID: " + entity.MentionId + "\n";
            output += "Confidence: " + entity.Confidence + "\n";
            output += "ID: " + entity.Id + "\n";


            if (entity.NormalizedValue != null)
            {
                output += "Normalized Value: " + entity.NormalizedValue.Text + "\n";
            }


            output += "Redacted: " + entity.Redacted + "\n";


            if (entity.Properties != null && entity.Properties.Count > 0)
            {
                output += "Nested Entities:\n";


                foreach (var nestedEntity in entity.Properties)
                {
                    output += "\tType: " + nestedEntity.Type + "\n";
                    output += "\tMention Text: " + nestedEntity.MentionText + "\n";
                    output += "\tID: " + nestedEntity.Id + "\n";
                    output += "\tRedacted: " + nestedEntity.Redacted + "\n";
                }
            }

            output += "\n";
        }

        Console.WriteLine(output);

        DateTime now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time"));
        string guid = Guid.NewGuid().ToString();


        // Upload results to files
        var client = StorageClient.Create();
        var content = Encoding.UTF8.GetBytes(output.ToString());
        var obj1 = client.UploadObject(outputstorage, guid+"-Response.txt", "text/plain", new MemoryStream(content));
    }


    /* Calling Document AI API */
    private async Task<Document> ProcessDocumentAsync(Uri uri, CancellationToken cancellationToken)
    {
        var bucketName = uri.Host;
        var objectName = uri.AbsolutePath.TrimStart('/');
        string contentType = GetContentType(objectName);
        Console.WriteLine(objectName);

        if(contentType == string.Empty)
        {
            LogError(new Exception($"Invalid file type. Cannot process. ObjectName is : {objectName}")); 
        }

        var obj = await _storageClient.GetObjectAsync(bucketName, objectName);

        using var memoryStream = new MemoryStream();
        _storageClient.DownloadObject(obj, memoryStream);
        memoryStream.Position = 0;

        var rawDocument = new RawDocument
        {
            Content = ByteString.FromStream(memoryStream),
            MimeType = contentType
        };


        var processorName = new ProcessorName(projectId, gcp_Region, documentAI_processor);


        var request = new ProcessRequest
        {
            RawDocument = rawDocument,
            Name = processorName.ToString()
        };


        var response = await _documentAI.ProcessDocumentAsync(request);
        return response.Document;
    }

   /* Convery Document Object to Custom Database Object */
    public static DBObject ConvertDocumentToCustomObjects(Document document)
    {
        var invoice = new Invoice();
        var items = new List<Item>();
        var tempDBObject = new DBObject();

        foreach (var entity in document.Entities)
        {
            if (String.Compare(entity.Type, "InvoiceID", StringComparison.Ordinal) == 0)
            {
                invoice.InvoiceID = entity.NormalizedValue != null ? entity.NormalizedValue.Text : entity.MentionText;         
            }
            if (String.Compare(entity.Type, "Items_sold_per_invoice", StringComparison.Ordinal) == 0)
            {  
                string input = entity.NormalizedValue != null ? entity.NormalizedValue.Text : entity.MentionText;
            
                int result;
                if (int.TryParse(input, out result))
                {
                    invoice.Items_sold_per_invoice = result;
                }
                else
                {
                    LogError(new Exception($"Unable to convert Items_sold_per_invoice input to int.{input}"));
                }      
            }
            else if(String.Compare(entity.Type, "Purchase_Date", StringComparison.Ordinal) == 0)
            {
                string input = entity.MentionText.Trim();
                
                DateTime result;
                if (DateTime.TryParse(input, out result))
                {
                    invoice.Purchase_Date = result;
                }
                else
                {
                     LogError(new Exception($"Unable to convert Purchase_Date input to DateTime.{input}"));
                }
            }
            else if(String.Compare(entity.Type, "Sales_Tax", StringComparison.Ordinal) == 0)
            {
                var input = entity.NormalizedValue != null ? entity.NormalizedValue.Text : entity.MentionText;

                float result;
                if (float.TryParse(input, out result))
                {
                    invoice.Sales_Tax = result;
                }
                else
                {
                    Console.WriteLine($"Unable to convert Sales_Tax input to float.{input}");
                }
            }
            else if(String.Compare(entity.Type, "SubTotal", StringComparison.Ordinal) == 0)
            {
                var input = entity.NormalizedValue != null ? entity.NormalizedValue.Text : entity.MentionText;
                input = input.TrimEnd('-');
                float result;

                if (float.TryParse(input, out result))
                {
                    invoice.SubTotal = result;
                }
                else
                {
                    LogError(new Exception($"Unable to convert SubTotal input to float.{input}"));
                }
            }
            else if(String.Compare(entity.Type, "Purchased_Items", StringComparison.Ordinal) == 0)
            {
                if (entity.Properties != null && entity.Properties.Count > 0)
                {
                    var tempitem = new Item();
                    foreach (var nestedEntity in entity.Properties)
                    {
                        if (String.Compare(nestedEntity.Type, "Item_code", StringComparison.Ordinal) == 0)
                        {
                            tempitem.Item_code = nestedEntity.NormalizedValue != null ? nestedEntity.NormalizedValue.Text : nestedEntity.MentionText;
                        }
                        else if(String.Compare(nestedEntity.Type, "Item_cost", StringComparison.Ordinal) == 0)
                        {
                            var input = nestedEntity.NormalizedValue != null ? nestedEntity.NormalizedValue.Text : nestedEntity.MentionText;
                            input = input.TrimEnd('-');
                            float result;

                            if (float.TryParse(input, out result))
                            {
                                tempitem.Item_cost = result;
                            }
                            else
                            {
                                LogError(new Exception($"Unable to convert Item_cost input to float.{input}"));
                            }
                        } 
                    }
                    items.Add(tempitem);
                }
            }
        }

        tempDBObject.InvoiceObj = invoice;
        tempDBObject.ItemsObj = items;
        return tempDBObject; 
    }

    /* Insert DBObject to BQ for analysis */
    private static void InsertDBObjectToBQ(DBObject tempDBObject)
    {
        // Insert a row into BQ invoice table
        InsertInvoiceEntityToBQ(tempDBObject.InvoiceObj, "SupermarketX", "Invoices");

        // Insert rows into BQ invoice Item table
        InsertItemsEntityToBQ(tempDBObject.InvoiceObj.InvoiceID, tempDBObject.ItemsObj, "SupermarketX", "Items"); 
    }

    /* Insert invoice object DBObject to Datastore to web or mobile interface */
    private static void InsertEntitiesToDatastore(DBObject tempDBObject)
    {
        InsertInvoiceEntityToDatastore(tempDBObject.InvoiceObj,"Invoices");

        InsertItemsEntityToDatastore(tempDBObject.InvoiceObj.InvoiceID, tempDBObject.ItemsObj, "Items");
    }

    /* Insert invoice object to Datastore */
    public static void InsertInvoiceEntityToDatastore(Invoice invoice, string invoiceKindName)
    {
        var client = DatastoreDb.Create(projectId);
        
        var invoiceEntity = new DsEntity
        {
            Key = client.CreateKeyFactory(invoiceKindName).CreateIncompleteKey()
        };

        // Convert purchase date to datetime stamp
        DateTime purchaseDateTime = invoice.Purchase_Date;
        if (purchaseDateTime.Kind != DateTimeKind.Utc)
        {
            purchaseDateTime = purchaseDateTime.ToUniversalTime();
        }
        Timestamp timestamp = Timestamp.FromDateTime(purchaseDateTime);


        invoiceEntity["Items_sold_per_invoice"] = invoice.Items_sold_per_invoice;
        invoiceEntity["Purchase_Date"] = timestamp;
        invoiceEntity["Sales_Tax"] = invoice.Sales_Tax;
        invoiceEntity["SubTotal"] =  invoice.SubTotal;
        invoiceEntity["InvoiceID"] = invoice.InvoiceID.Trim();
        client.Upsert(invoiceEntity);
    }

    /* Insert Item objects to Datastore */
    public static void InsertItemsEntityToDatastore(string invoiceID, List<Item> items, string invoiceKindName)
    {
        var client = DatastoreDb.Create(projectId);
        var itemEntities = new List<DsEntity>();
        var keyFactory = client.CreateKeyFactory(invoiceKindName);

        foreach(var item in items)
        {
            // Create a BigQuery insert row request
            DsEntity dsEntity = new DsEntity
            {
                Key = keyFactory.CreateIncompleteKey()
            };
            dsEntity["Item_code"] = item.Item_code;
            dsEntity["Item_cost"] = item.Item_cost;
            dsEntity["InvoiceID"] = invoiceID;
            itemEntities.Add(dsEntity);
        }  
        client.Upsert(itemEntities);
       
    }

    public static void InsertItemsEntityToBQ(string invoiceID, List<Item> items, string datasetId, string tableId)
    {
        BigQueryClient client = BigQueryClient.Create(projectId);

        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();

        foreach(var item in items)
        {
            // Create a BigQuery insert row request
            BigQueryInsertRow insertRow = new BigQueryInsertRow();
            insertRow.Add("Item_code", item.Item_code);
            insertRow.Add("InvoiceID",invoiceID);
            insertRow.Add("Item_cost",item.Item_cost);
            rows.Add(insertRow);
        }  

         // Insert rows into the BigQuery table
         client.InsertRows(datasetId, tableId, rows);

    }

    public static void InsertInvoiceEntityToBQ(Invoice invoice, string datasetId, string tableId)
    {
        BigQueryClient client = BigQueryClient.Create(projectId);
        
        // Create a BigQuery insert row request
        BigQueryInsertRow insertRow = new BigQueryInsertRow();
        insertRow.Add("Items_sold_per_invoice", invoice.Items_sold_per_invoice);
        insertRow.Add("Purchase_Date",invoice.Purchase_Date);
        insertRow.Add("Sales_Tax",invoice.Sales_Tax);
        insertRow.Add("SubTotal",  invoice.SubTotal);
        insertRow.Add("InvoiceID", invoice.InvoiceID.Trim());

        // Insert the row into the BigQuery table
        client.InsertRow(datasetId, tableId, insertRow);
    }


    public static void LogError(Exception ex)
    {
        // Initialize the Logging client
        LoggingServiceV2Client loggingClient = LoggingServiceV2Client.Create();

        // Prepare new log entry.
        LogEntry logEntry = new LogEntry();
        LogName logName = new LogName(projectId, logId);
        logEntry.LogNameAsLogName = logName;
        logEntry.Severity = LogSeverity.Error;
        logEntry.TextPayload = ex.ToString();

        // Write the log entry to Google Cloud Logging
        MonitoredResource resource = new MonitoredResource { Type = "global" };
        try
        {
            // Create dictionary object to add custom labels to the log entry.
            IDictionary<string, string> entryLabels = new Dictionary<string, string>();

            // Add log entry to collection for writing. Multiple log entries can be added.
            IEnumerable<LogEntry> logEntries = new LogEntry[] { logEntry };

            // Write new log entry.
            loggingClient.WriteLogEntries(logName, resource, entryLabels, logEntries);
            //loggingClient.WriteLogEntries(resource, new[] { logEntry });
        }
        catch (RpcException e) when (e.Status.StatusCode == StatusCode.Unauthenticated)
        {
            Console.WriteLine("Failed to authenticate: " + e);
        }
        catch (Exception e)
        {
            Console.WriteLine("Failed to write log: " + e);
        }
    }

    public static string GetContentType(string fileName)
    {
        // Get the file extension
        string extension = Path.GetExtension(fileName).ToLowerInvariant();
        switch (extension)
        {
            case ".pdf":
                return "application/pdf";
            case ".jpg":
            case ".jpeg":
            case ".png":
                return "image/" + extension.Substring(1);
            default:
                return string.Empty;
        }
    }
}
