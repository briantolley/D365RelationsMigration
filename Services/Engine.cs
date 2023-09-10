using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Net.Http.Headers;
using Microsoft.Identity.Client;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using System.Configuration;
using System.Xml;
using static System.Windows.Forms.AxHost;

namespace Relationship2ConnectionMigration.Services
{
    internal class Engine
    {

        //  Definition:
        //  This engine goes through the process of preparing, cleansing, transforming and processing all need changes to a target D365 RDBMS instance
        //  to migrate relationship entities to connection entities due to the pending deprecation by MSFT announced on 06/26/2023.
        //  The entire approach is black box using an encapsulator design pattern to abstract the complexity and is self reliant.

        public async Task<bool> Run()
        {
            //  Behavior:
            //  This is the ONLY public explosed behavior to the class.

            //  1.  Define the variables to be used in the scope of the lifetime of the stack and child calls.
            SqlConnection sqlreadconnection;
            SqlConnection sqllookupconnection;
            SqlConnection sqlcleanupconnection;
            DateTime starttime;
            DateTime endtime;
            TimeSpan runtime;

            //Console.WriteLine(HubSpotConnection());

            //  2.  Make the connection to the database here now to continue processing.
            starttime = DateTime.Now;
            Console.WriteLine(starttime.ToString() + " is the time cleanup processing began.");
            sqlreadconnection = new SqlConnection(ConfigurationManager.AppSettings["D365RDBMSConnectionString"]);
            //sqlreadconnection = new SqlConnection("Persist Security Info=false;User ID=dev\\crminstaller;Password=Hello123!;Initial Catalog=AAFMAA_MSCRM;Server=DEV-CRM90-SQL;Pooling=false;MultipleActiveResultSets=false;");
            sqlreadconnection.Open();
            sqllookupconnection = new SqlConnection(sqlreadconnection.ConnectionString);
            sqllookupconnection.Open();
            sqlcleanupconnection = new SqlConnection(sqlreadconnection.ConnectionString);
            sqlcleanupconnection.Open();

            //  3.  Dedupe relationships to insure uniqueness in the data here now.
            //Console.WriteLine("Starting Dedupe Process at " + DateTime.Now.ToString() + ".");
            //await DedupeRelationships(sqlreadconnection, sqlcleanupconnection);
            //Console.WriteLine("Finished Dedupe Process at " + DateTime.Now.ToString() + ".");

            //  4.  Now make sure the connection roles all match the relationship roles here.
            // Console.WriteLine("Starting Role Sync Relationships to Connections Process at " + DateTime.Now.ToString() + ".");
            // await SyncRoles(sqlreadconnection, sqllookupconnection, sqlcleanupconnection);
            //Console.WriteLine("Finished Role Sync Relationships to Connections Process at " + DateTime.Now.ToString() + ".");

            //  5.  Now make sure the connection roles all match the relationship roles here.
            Console.WriteLine("Starting Sync Relationships to Connections Migration at " + DateTime.Now.ToString() + ".");
            //await MigrateRelationshipsPremise(sqlreadconnection, sqllookupconnection, sqlcleanupconnection);
            await MigrateRelationshipsCloudAsync(sqlreadconnection, sqlcleanupconnection);
            Console.WriteLine("Finished Sync Relationships to Connections Migration at " + DateTime.Now.ToString() + ".");

            endtime = DateTime.Now;
            Console.WriteLine(endtime.ToString() + " is the time cleanup processing completed.");
            runtime = endtime.Subtract(starttime);
            Console.WriteLine(runtime.TotalMinutes.ToString() + " minutes taken to run.");

            //  Z.  Close the database connection now.
            sqlcleanupconnection.Close();
            sqllookupconnection.Close();
            sqlreadconnection.Close();
            return true;

        }
        private async Task<bool> DedupeRelationships(SqlConnection sqlreadconnection, SqlConnection sqlcleanupconnection)
        {
            //  Behavior:
            //  This is the ONLY public explosed behavior to the class.

            //  1.  Define the variables to be used in the scope of the lifetime of the stack and child calls.
            SqlCommand sqlcommand;
            SqlCommand sqlcleanupcommand;
            SqlDataReader sqlreader;

            //  2.  To start with we need to create a master list of relationships without carrying the recursive relationship with it.
            try
            { 
                sqlcommand = new SqlCommand("DROP TABLE MigrationRelationship2ConnectionUnique", sqlreadconnection);
                sqlcommand.ExecuteNonQuery();
                sqlcommand = new SqlCommand("DROP TABLE MigrationRelationship2Connection", sqlreadconnection);
                sqlcommand.ExecuteNonQuery();
            }
            catch
            {
            }
            sqlcommand = new SqlCommand("CREATE TABLE MigrationRelationship2ConnectionUnique (Id int IDENTITY(1, 1), CustomerId uniqueidentifier, Total int)", sqlreadconnection);
            sqlcommand.ExecuteNonQuery();
            sqlcommand = new SqlCommand("CREATE TABLE MigrationRelationship2Connection (Id int IDENTITY(1,1), CustomerId uniqueidentifier, PartnerId uniqueidentifier, Syncd int)", sqlreadconnection);
            sqlcommand.ExecuteNonQuery();

            //  3.  Now we want to populate the data accordingly to the system.
            sqlcommand = new SqlCommand("INSERT INTO MigrationRelationship2ConnectionUnique (CustomerId, Total) SELECT CustomerId, COUNT(*) AS Total FROM CustomerRelationshipBase WITH (NOLOCK) GROUP BY CustomerId ORDER BY Total DESC", sqlreadconnection);
            sqlcommand.ExecuteNonQuery();
            sqlcommand = new SqlCommand("INSERT INTO MigrationRelationship2Connection (CustomerId, PartnerId, Syncd) SELECT CustomerRelationshipBase.CustomerId, CustomerRelationshipBase.PartnerId, 0 AS Syncd FROM CustomerRelationshipBase WITH (NOLOCK) INNER JOIN MigrationRelationship2ConnectionUnique WITH (NOLOCK) ON CustomerRelationshipBase.CustomerId = MigrationRelationship2ConnectionUnique.CustomerId ORDER BY MigrationRelationship2ConnectionUnique.Id", sqlreadconnection);
            sqlcommand.ExecuteNonQuery();

            //  4.  Now we need to make sure to delete any recursive relationships of one to many from the system.
            //sqlcleanupcommand = new SqlCommand("DELETE FROM MigrationRelationship2Connection WHERE PartnerId IN (SELECT DISTINCT(CustomerId) FROM MigrationRelationship2Connection WITH (NOLOCK) GROUP BY CustomerId HAVING (COUNT(PartnerId) > 1))", sqlcleanupconnection);
            //sqlcleanupcommand.ExecuteNonQuery();

            //  5.  Now we need to make sure to delete any recursive one to one relationships from the system.
            sqlcommand = new SqlCommand("CREATE INDEX MigrationCustomerId ON MigrationRelationship2Connection (Id)", sqlreadconnection);
            sqlcommand.ExecuteNonQuery();
            sqlcommand = new SqlCommand("CREATE CLUSTERED INDEX MigrationCustomerRelationship ON MigrationRelationship2Connection (CustomerId, PartnerId)", sqlreadconnection);
            sqlcommand.ExecuteNonQuery();
            try
            {
                sqlcleanupcommand = new SqlCommand("DROP PROCEDURE [dbo].[MigrationOneToOneRecursives]", sqlcleanupconnection);
                sqlcleanupcommand.ExecuteNonQuery();
            }
            catch
            {
            }
            sqlcleanupcommand = new SqlCommand("CREATE PROCEDURE [dbo].[MigrationOneToOneRecursives] AS BEGIN DECLARE @CustomerId varchar(500) = ''; DECLARE @PartnerId varchar(500) = ''; DECLARE @Id int = 0; SELECT TOP (1) @Id = Id, @CustomerId = CustomerId, @PartnerId = PartnerId FROM MigrationRelationship2Connection WITH (NOLOCK) ORDER BY Id; WHILE @CustomerId <>'' BEGIN DELETE FROM MigrationRelationship2Connection WHERE PartnerId = @CustomerId AND CustomerId = @PartnerId; SET @CustomerId = ''; SET @PartnerId = ''; SELECT TOP (1) @Id = Id, @CustomerId = CustomerId, @PartnerId = PartnerId FROM MigrationRelationship2Connection WITH (NOLOCK) WHERE (Id > @Id) ORDER BY Id; END; END", sqlcleanupconnection);
            sqlcleanupcommand.CommandTimeout = 0;
            sqlcleanupcommand.ExecuteNonQuery();
            sqlcleanupcommand = new SqlCommand("exec MigrationOneToOneRecursives", sqlcleanupconnection);
            sqlcleanupcommand.CommandTimeout = 0;
            sqlcleanupcommand.ExecuteNonQuery();

            //  6.  Get the final count now that everything has been done and we are ready to move the data over.
            sqlcommand = new SqlCommand("SELECT count(*) FROM MigrationRelationship2Connection WITH (NOLOCK)", sqlreadconnection);
            sqlreader = sqlcommand.ExecuteReader();
            sqlreader.Read();
            Console.WriteLine(sqlreader.GetValue(0).ToString() + " clean unique relationships ready to import as connections.");
            sqlreader.Close();

            //  8.  Close the database connection now.
            return true;

        }
        private async Task<bool> SyncRoles(SqlConnection sqlreadconnection, SqlConnection sqllookupconnection, SqlConnection sqlcleanupconnection)
        {
            //  Behavior:
            //  This is the private method works to insure all the roles have been migrated from relationship to connection.

            //  1.  Define the variables to be used in the scope of the lifetime of the stack and child calls.
            SqlCommand sqlcommand;
            SqlCommand sqllookupcommand;
            SqlCommand sqlinsertcommand;
            SqlDataReader sqlreader;
            SqlDataReader sqllookupreader;
            SqlTransaction sqltransaction;
            bool roleneedsadded;
            Guid roleguid;

            //  2.  Fetch all the relationships that are currently defined and in use in the system.
            sqlcommand = new SqlCommand("SELECT DISTINCT RelationshipRoleBase.Name, RelationshipRoleBase.StatusCode, RelationshipRoleBase.StateCode, RelationshipRoleBase.OrganizationId, RelationshipRoleBase.VersionNumber, RelationshipRoleBase.CreatedOn, RelationshipRoleBase.ModifiedOn FROM CustomerRelationshipBase INNER JOIN RelationshipRoleBase ON CustomerRelationshipBase.CustomerRoleId = RelationshipRoleBase.RelationshipRoleId UNION SELECT DISTINCT RelationshipRoleBase.Name, RelationshipRoleBase.StatusCode, RelationshipRoleBase.StateCode, RelationshipRoleBase.OrganizationId, RelationshipRoleBase.VersionNumber, RelationshipRoleBase.CreatedOn, RelationshipRoleBase.ModifiedOn FROM CustomerRelationshipBase INNER JOIN RelationshipRoleBase ON CustomerRelationshipBase.PartnerRoleId = RelationshipRoleBase.RelationshipRoleId", sqlreadconnection);
            sqlreader = sqlcommand.ExecuteReader();
            while (sqlreader.Read())
            {

                //  3.  Start by checking to make sure that the connection role is not already there.
                sqllookupcommand = new SqlCommand("SELECT Name FROM ConnectionRoleBase WHERE Name = '" + Regex.Replace(sqlreader.GetValue(0).ToString(), "'", "''") + "'", sqllookupconnection);
                sqllookupreader = sqllookupcommand.ExecuteReader();
                roleneedsadded = !sqllookupreader.HasRows;
                sqllookupreader.Close();
                if (roleneedsadded)
                {
                    //  4.  If we fine that the connection role has not been defined then we will create that connection role now.
                    sqltransaction = sqlcleanupconnection.BeginTransaction();
                        roleguid = Guid.NewGuid();
                        sqlinsertcommand = new SqlCommand("INSERT INTO [ConnectionRoleBase] ([ConnectionRoleId],[CreatedBy],[CreatedOn],[ComponentState],[IsManaged],[OrganizationId],[ModifiedOn],[Name],[StatusCode],[StateCode],[Category],[ModifiedBy],[OverwriteTime],[IsCustomizable],[SolutionId],[IntroducedVersion]) VALUES ('" + roleguid.ToString() + "', '152EAC5B-725F-DF11-B0F6-00219B936B57', GETDATE(), 0, 0, '" + sqlreader.GetValue(3).ToString() + "', '" + sqlreader.GetValue(6).ToString() + "', '" + Regex.Replace(sqlreader.GetValue(0).ToString(), "'", "''") + "', 1, 0, 2, '54B2291C-2111-EE11-B859-005056A84805', '1900-01-01 00:00:00.000', 1, 'FD140AAE-4DF4-11DD-BD17-0019B9312238', '1.0')", sqlcleanupconnection);
                        sqlinsertcommand.Transaction = sqltransaction;
                        sqlinsertcommand.ExecuteNonQuery();
                        sqlinsertcommand = new SqlCommand("INSERT INTO [ConnectionRoleBaseIds] SELECT '" + roleguid.ToString() + "'", sqlcleanupconnection);
                        sqlinsertcommand.Transaction = sqltransaction;
                        sqlinsertcommand.ExecuteNonQuery();
                        sqlinsertcommand = new SqlCommand("INSERT INTO [ConnectionRoleObjectTypeCodeBase] ([AssociatedObjectTypeCode],[ConnectionRoleId],[ConnectionRoleObjectTypeCodeId]) VALUES (2, '" + roleguid.ToString() + "', NEWID())", sqlcleanupconnection);
                        sqlinsertcommand.Transaction = sqltransaction;
                        sqlinsertcommand.ExecuteNonQuery();
                    sqltransaction.Commit();
                }
                sqllookupreader.Close();
            }
            sqlreader.Close();

            //  Z.  Close the database connection now.
            return true;

        }
        private async Task<bool> MigrateRelationshipsPremise(SqlConnection sqlreadconnection, SqlConnection sqllookupconnection, SqlConnection sqlcleanupconnection)
        {
            //  Behavior:
            //  This is the private method works to insure all the relationships have been migrated from to connections.

            //  1.  Define the variables to be used in the scope of the lifetime of the stack and child calls.
            SqlCommand sqlcommand;

            //  2.  As there should be no connection that were rleationships it must all be garbage so wipe it out and migrate it over.
            try
            {
                sqlcommand = new SqlCommand("DROP PROCEDURE [dbo].[MigrationMoveRelationshipsToConnections]", sqlcleanupconnection);
                sqlcommand.ExecuteNonQuery();
            }
            catch
            {
            }
            sqlcommand = new SqlCommand("CREATE PROCEDURE [dbo].[MigrationMoveRelationshipsToConnections] AS BEGIN DECLARE @connectionprimaryid uniqueidentifier; DECLARE @connectionsecondaryid uniqueidentifier; DECLARE @cursorCustomerId varchar(500); DECLARE @cursorCustomerIdName varchar(500); DECLARE @cursorCustomerRoleId varchar(500); DECLARE @cursorPartnerId varchar(500); DECLARE @cursorPartnerIdIdName varchar(500); DECLARE @cursorPartnerIdRoleId varchar(500); DECLARE @cursorCreatedOn varchar(500); SET NOCOUNT ON DELETE FROM ConnectionBase WHERE  (Record1ObjectTypeCode = 2) AND (Record2ObjectTypeCode = 2); DECLARE myCursor CURSOR FOR SELECT MigrationRelationship2Connection.CustomerId, CustomerRelationshipBase.CustomerIdName, ConnectionRoleBase.ConnectionRoleId, MigrationRelationship2Connection.PartnerId, CustomerRelationshipBase.PartnerIdName, ConnectionRoleBase_1.ConnectionRoleId AS PartnerRoleId, CustomerRelationshipBase.CreatedOn FROM ConnectionRoleBase AS ConnectionRoleBase_1 WITH (NOLOCK) INNER JOIN RelationshipRoleBase AS RelationshipRoleBase_1 WITH (NOLOCK) ON ConnectionRoleBase_1.Name = RelationshipRoleBase_1.Name INNER JOIN ConnectionRoleBase WITH (NOLOCK) INNER JOIN RelationshipRoleBase WITH (NOLOCK) INNER JOIN MigrationRelationship2Connection WITH (NOLOCK) INNER JOIN CustomerRelationshipBase WITH (NOLOCK) ON MigrationRelationship2Connection.CustomerId = CustomerRelationshipBase.CustomerId AND MigrationRelationship2Connection.PartnerId = CustomerRelationshipBase.PartnerId ON RelationshipRoleBase.RelationshipRoleId = CustomerRelationshipBase.CustomerRoleId ON ConnectionRoleBase.Name = RelationshipRoleBase.Name ON RelationshipRoleBase_1.RelationshipRoleId = CustomerRelationshipBase.PartnerRoleId ORDER BY MigrationRelationship2Connection.CustomerId ASC OPEN myCursor; FETCH NEXT FROM myCursor INTO @cursorCustomerId, @cursorCustomerIdName, @cursorCustomerRoleId, @cursorPartnerId, @cursorPartnerIdIdName, @cursorPartnerIdRoleId, @cursorCreatedOn; WHILE @@FETCH_STATUS = 0 BEGIN BEGIN TRAN SET @connectionprimaryid = NEWID(); SET @connectionsecondaryid = NEWID(); INSERT INTO [ConnectionBase] ([IsMaster], [Record1ObjectTypeCode], [OwningBusinessUnit], [ModifiedOnBehalfBy], [Record2RoleId], [Record1Id], [StateCode], [ConnectionId], [StatusCode], [ImportSequenceNumber], [OwnerIdType], [CreatedBy], [Record2ObjectTypeCode], [CreatedOn], [TransactionCurrencyId], [OwnerId], [Record2IdObjectTypeCode], [Record1RoleId], [ModifiedBy], [Record1IdObjectTypeCode], [ExchangeRate], [ModifiedOn], [Record2Id]) VALUES (1, 2, 'FA1CAC5B-725F-DF11-B0F6-00219B936B57', NULL, @cursorCustomerRoleId, @cursorPartnerId, 0, CONVERT(VARCHAR(500), @connectionsecondaryid), 1, 7777, 8, '152EAC5B-725F-DF11-B0F6-00219B936B57', 2, @cursorCreatedOn, '2EF4D774-725F-DF11-B0F6-00219B936B57', '152EAC5B-725F-DF11-B0F6-00219B936B57', 2, @cursorPartnerIdRoleId, '152EAC5B-725F-DF11-B0F6-00219B936B57', 2, 1.0000000000, GETDATE(), @cursorCustomerId); INSERT INTO [ConnectionBase] ([IsMaster], [Record1ObjectTypeCode], [OwningBusinessUnit], [ModifiedOnBehalfBy], [Record2RoleId], [Record1Id], [StateCode], [ConnectionId], [StatusCode], [ImportSequenceNumber], [OwnerIdType], [CreatedBy], [Record2ObjectTypeCode], [CreatedOn], [TransactionCurrencyId], [OwnerId], [Record2IdObjectTypeCode], [Record1RoleId], [ModifiedBy], [Record1IdObjectTypeCode], [ExchangeRate], [ModifiedOn], [Record2Id]) VALUES (0, 2, 'FA1CAC5B-725F-DF11-B0F6-00219B936B57', NULL, @cursorPartnerIdRoleId, @cursorCustomerId, 0, CONVERT(VARCHAR(500), @connectionprimaryid), 1, 7777, 8, '152EAC5B-725F-DF11-B0F6-00219B936B57', 2, @cursorCreatedOn, '2EF4D774-725F-DF11-B0F6-00219B936B57', '152EAC5B-725F-DF11-B0F6-00219B936B57', 2, @cursorCustomerRoleId, '152EAC5B-725F-DF11-B0F6-00219B936B57', 2, 1.0000000000, GETDATE(), @cursorPartnerId); UPDATE [ConnectionBase] set [RelatedConnectionId] = CONVERT(VARCHAR(500), @connectionprimaryid), [Record1IdName] = @cursorPartnerIdIdName, [Record2IdName] = @cursorCustomerIdName WHERE [ConnectionId] =  CONVERT(VARCHAR(500), @connectionsecondaryid); UPDATE [ConnectionBase] set [RelatedConnectionId] = CONVERT(VARCHAR(500), @connectionsecondaryid), [Record1IdName] = @cursorCustomerIdName, [Record2IdName] = @cursorPartnerIdIdName WHERE [ConnectionId] =  CONVERT(VARCHAR(500), @connectionprimaryid); COMMIT TRAN FETCH NEXT FROM myCursor INTO @cursorCustomerId, @cursorCustomerIdName, @cursorCustomerRoleId, @cursorPartnerId, @cursorPartnerIdIdName, @cursorPartnerIdRoleId, @cursorCreatedOn; END; CLOSE myCursor; DEALLOCATE myCursor; END", sqlcleanupconnection);
            sqlcommand.CommandTimeout = 0;
            sqlcommand.ExecuteNonQuery();
            sqlcommand = new SqlCommand("exec MigrationMoveRelationshipsToConnections", sqlcleanupconnection);
            sqlcommand.CommandTimeout = 0;
            sqlcommand.ExecuteNonQuery();

            //  Z.  Close the database connection now.
            return true;

        }
        public async Task<bool> MigrateRelationshipsCloudAsync(SqlConnection sqlreadconnection, SqlConnection sqlcleanupconnection)
        {
            //  Behavior:
            //  This is the private method works to fetch all relationships that are de-duped and being the migration process to the cloud via web api.

            //  1.  Define the variables to be used in the scope of the lifetime of the stack and child calls.
            HttpClient d365client;
            SqlCommand sqlcommand;
            SqlCommand sqlcleanupcommand;
            SqlDataReader sqlreader;
            HttpResponseMessage createConnectionResponse;
            SortedDictionary<string, string> connectionroles;
            string unittesting = "";
            int counter = 0;
            DateTime revalidatetime = DateTime.Now;
            TimeSpan revalidategap;
            DateTime splitstarttime;
            DateTime splitendtime;
            TimeSpan splittotaltime;
            TimeSpan splittotallasttime;
            //string nametosync = "IN (SELECT DISTINCT FullName FROM ContactBase AS ContactBase_1 WHERE (New_cn IN (N'21607')))";

            //  2.  Make the connection to the data verse D365 server for doing entity updates of connection here now.
            d365client = D365Connection(out revalidatetime);
            connectionroles = await CloudRoles(d365client);

            //  3.  Fetch all the relationships that need to be migrated here now.
            unittesting = ConfigurationManager.AppSettings["D365RDBMS2MSFTAzurePartitionClause"];
            //unittesting = $"WHERE (CustomerRelationshipBase.CustomerIdName {nametosync} OR CustomerRelationshipBase.PartnerIdName {nametosync})";
            //unittesting = "WHERE (ContactBase.CreatedOn >= '1/1/22' and ContactBase.CreatedOn < '1/1/23') AND (ContactBase.New_cn IS NOT NULL)";
            //unittesting = "WHERE (ContactBase.CreatedOn >= '1/1/19' and ContactBase.CreatedOn < '1/1/20') AND (ContactBase.New_cn IS NOT NULL)";
            //sqlcommand = new SqlCommand("SELECT TOP 10 MigrationRelationship2Connection.CustomerId, CustomerRelationshipBase.CustomerIdName, ConnectionRoleBase.ConnectionRoleId, MigrationRelationship2Connection.PartnerId, CustomerRelationshipBase.PartnerIdName, ConnectionRoleBase_1.ConnectionRoleId AS PartnerRoleId, CustomerRelationshipBase.CreatedOn, ConnectionRoleBase.Name AS CustomerRoleName, ConnectionRoleBase_1.Name AS PartnerRoleName FROM ConnectionRoleBase AS ConnectionRoleBase_1 WITH (NOLOCK) INNER JOIN RelationshipRoleBase AS RelationshipRoleBase_1 WITH (NOLOCK) ON ConnectionRoleBase_1.Name = RelationshipRoleBase_1.Name INNER JOIN ConnectionRoleBase WITH (NOLOCK) INNER JOIN RelationshipRoleBase WITH (NOLOCK) INNER JOIN MigrationRelationship2Connection WITH (NOLOCK) INNER JOIN CustomerRelationshipBase WITH (NOLOCK) ON MigrationRelationship2Connection.CustomerId = CustomerRelationshipBase.CustomerId AND MigrationRelationship2Connection.PartnerId = CustomerRelationshipBase.PartnerId ON RelationshipRoleBase.RelationshipRoleId = CustomerRelationshipBase.CustomerRoleId ON ConnectionRoleBase.Name = RelationshipRoleBase.Name ON RelationshipRoleBase_1.RelationshipRoleId = CustomerRelationshipBase.PartnerRoleId ORDER BY MigrationRelationship2Connection.CustomerId", sqlreadconnection);
            //sqlcommand = new SqlCommand("SELECT DISTINCT MigrationRelationship2Connection.CustomerId, CustomerRelationshipBase.CustomerIdName, ConnectionRoleBase.ConnectionRoleId, MigrationRelationship2Connection.PartnerId, CustomerRelationshipBase.PartnerIdName, ConnectionRoleBase_1.ConnectionRoleId AS PartnerRoleId, CustomerRelationshipBase.CreatedOn, ConnectionRoleBase.Name AS CustomerRoleName, ConnectionRoleBase_1.Name AS PartnerRoleName FROM ConnectionRoleBase AS ConnectionRoleBase_1 WITH (NOLOCK) INNER JOIN RelationshipRoleBase AS RelationshipRoleBase_1 WITH (NOLOCK) ON ConnectionRoleBase_1.Name = RelationshipRoleBase_1.Name INNER JOIN ConnectionRoleBase WITH (NOLOCK) INNER JOIN RelationshipRoleBase WITH (NOLOCK) INNER JOIN MigrationRelationship2Connection WITH (NOLOCK) INNER JOIN CustomerRelationshipBase WITH (NOLOCK) ON MigrationRelationship2Connection.CustomerId = CustomerRelationshipBase.CustomerId AND MigrationRelationship2Connection.PartnerId = CustomerRelationshipBase.PartnerId ON RelationshipRoleBase.RelationshipRoleId = CustomerRelationshipBase.CustomerRoleId ON ConnectionRoleBase.Name = RelationshipRoleBase.Name ON RelationshipRoleBase_1.RelationshipRoleId = CustomerRelationshipBase.PartnerRoleId INNER JOIN ContactBase WITH (NOLOCK) ON CustomerRelationshipBase.CustomerId =  ContactBase.ContactId " + unittesting + " ORDER BY MigrationRelationship2Connection.CustomerId", sqlreadconnection);
            sqlcommand = new SqlCommand("SELECT DISTINCT MigrationRelationship2Connection.CustomerId, CustomerRelationshipBase.CustomerIdName, '' AS ConnectionRoleId, MigrationRelationship2Connection.PartnerId, CustomerRelationshipBase.PartnerIdName, '' AS PartnerRoleId, CustomerRelationshipBase.CreatedOn, RelationshipRoleBase.Name AS CustomerRoleName, RelationshipRoleBase_1.Name AS PartnerRoleName, MigrationRelationship2Connection.Id FROM RelationshipRoleBase AS RelationshipRoleBase_1 WITH (NOLOCK) INNER JOIN RelationshipRoleBase WITH (NOLOCK) INNER JOIN MigrationRelationship2Connection WITH (NOLOCK) INNER JOIN CustomerRelationshipBase WITH (NOLOCK) ON MigrationRelationship2Connection.CustomerId = CustomerRelationshipBase.CustomerId AND MigrationRelationship2Connection.PartnerId = CustomerRelationshipBase.PartnerId ON RelationshipRoleBase.RelationshipRoleId = CustomerRelationshipBase.CustomerRoleId ON RelationshipRoleBase_1.RelationshipRoleId = CustomerRelationshipBase.PartnerRoleId INNER JOIN ContactBase WITH (NOLOCK) ON CustomerRelationshipBase.CustomerId = ContactBase.ContactId " + unittesting + "ORDER BY MigrationRelationship2Connection.Id", sqlreadconnection);
            sqlreader = sqlcommand.ExecuteReader();
            splitstarttime = DateTime.Now;
            splittotallasttime = DateTime.Now.Subtract(DateTime.Now);
            while (sqlreader.Read())
            {

                //  *.  If things run for so long we need to check to see if the revalidate time is getting close and IF it is then we need to reset the token.
                revalidategap = revalidatetime - DateTime.Now;
                if (revalidategap.TotalMinutes < 30)
                    { d365client = D365Connection(out revalidatetime); }


                //  4.  Now we will address pushing of relationships to request connection adds in the data verse.
                if (counter % 200 == 0)
                {
                    splitendtime = DateTime.Now;
                    splittotaltime = splitendtime.Subtract(splitstarttime);
                    Console.WriteLine("Pace of load split currently is " + (Convert.ToDecimal(splittotaltime.Seconds) / Convert.ToDecimal(200)).ToString() + " records per second compared to " + (Convert.ToDecimal(splittotallasttime.Seconds) / Convert.ToDecimal(200)).ToString() + " on the last split.");
                    splitstarttime = DateTime.Now;
                    splittotallasttime = splittotaltime;
                }
                counter++;

                tryagain:

                Console.WriteLine(sqlreader.GetValue(1).ToString() + " => " + sqlreader.GetValue(4).ToString());
                //await EraseConnectionInCloud(d365client, sqlreader.GetValue(0).ToString(), sqlreader.GetValue(3).ToString());
                JObject connectiontocreate = new JObject
                {
                    { "record1id_contact@odata.bind", $"/contacts({sqlreader.GetValue(0).ToString()})" },
                    { "record2id_contact@odata.bind", $"/contacts({sqlreader.GetValue(3).ToString()})" },
                    { "record1roleid@odata.bind", $"/connectionroles({connectionroles[sqlreader.GetValue(7).ToString()].ToString()})" },
                    { "record2roleid@odata.bind", $"/connectionroles({connectionroles[sqlreader.GetValue(8).ToString()].ToString()})" }
                };
                HttpRequestMessage createRequest = new HttpRequestMessage(new HttpMethod("POST"), $"{d365client.BaseAddress.ToString()}v9.0/connections");
                createRequest.Content = new StringContent(connectiontocreate.ToString(), Encoding.UTF8, "application/json");
                try
                {
                    createConnectionResponse = await d365client.SendAsync(createRequest);
                    if (!createConnectionResponse.IsSuccessStatusCode)
                    {
                        JObject objError = JObject.Parse(createConnectionResponse.Content.ReadAsStringAsync().Result);
                        string errorMsg = objError.SelectToken("error.message")?.ToString();
                        if (errorMsg.Contains("already exists") == true)
                        {
                            sqlcleanupcommand = new SqlCommand($"UPDATE MigrationRelationship2Connection SET Syncd = -1 WHERE CustomerId = '{sqlreader.GetValue(0).ToString()}' and PartnerId = '{sqlreader.GetValue(3).ToString()}'", sqlcleanupconnection);
                            sqlcleanupcommand.ExecuteNonQuery();
                            Console.WriteLine("Record already in cloud.");
                        }
                        else
                        {
                            createConnectionResponse.StatusCode = HttpStatusCode.OK;
                        }
                    }
                    if (createConnectionResponse.IsSuccessStatusCode)
                    {
                        sqlcleanupcommand = new SqlCommand($"UPDATE MigrationRelationship2Connection SET Syncd = 1 WHERE CustomerId = '{sqlreader.GetValue(0).ToString()}' and PartnerId = '{sqlreader.GetValue(3).ToString()}'", sqlcleanupconnection);
                        sqlcleanupcommand.ExecuteNonQuery();
                    }
                }
                catch
                {
                    Console.WriteLine("Error");
                    d365client = D365Connection(out revalidatetime);
                    goto tryagain;
                }
            }
            sqlreader.Close();

            //  Z.  Close the database connection now.
            Console.WriteLine(counter.ToString() + " relationships pushed to the cloud as connections.");
            return true;

        }
        private async Task<SortedDictionary<string, string>> CloudRoles(HttpClient d365client)
        {
            //  Behavior:
            //  This is the private method works to fetch all relationships that are de-duped and being the migration process to the cloud via web api.

            //  1.  Define the variables to be used in the scope of the lifetime of the stack and child calls.
            JObject jsonconnectionrolesresponse = new JObject();
            JToken jsonconnectionroles;
            SortedDictionary<string, string> connectionroles = new SortedDictionary<string, string>();
            string connectionroleid = "";
            string connectionrolename = "";
            HttpResponseMessage connectionrolesresponse;

                HttpRequestMessage createRequest = new HttpRequestMessage(new HttpMethod("GET"), $"{d365client.BaseAddress.ToString()}v9.0/connectionroles");
                try
                {
                    connectionrolesresponse = await d365client.SendAsync(createRequest);
                    if (connectionrolesresponse.IsSuccessStatusCode)
                    {
                        jsonconnectionrolesresponse = JObject.Parse(connectionrolesresponse.Content.ReadAsStringAsync().Result);
                        jsonconnectionroles = jsonconnectionrolesresponse.Children().Last();
                        jsonconnectionroles = jsonconnectionroles.Children().First();
                        foreach (JObject connectionrole in jsonconnectionroles)
                        {
                            foreach (JProperty property in connectionrole.Children<JProperty>())
                            {
                                switch (property.Name.ToLower())
                                {
                                    case "connectionroleid" : 
                                        connectionroleid = property.Value.ToString();
                                        break;
                                    case "name" :
                                        connectionrolename = property.Value.ToString();
                                        break;
                                }
                            }
                            connectionroles.Add(connectionrolename, connectionroleid);
                        }
                    }
                }
                catch
                {

                };

            //  Z.  Close the database connection now.
            return connectionroles;

        }
        private async Task<bool> EraseConnectionInCloud(HttpClient d365client, string Id1, string Id2)
        {
            //  Behavior:
            //  This is the private method works to delete both scenarios where there is a connection in the database that needs to be removed.

            //  1.  Define the variables to be used in the scope of the lifetime of the stack and child calls.
            JObject jsonconnectionstoremove = new JObject();
            JToken jsonconnectiontoremove;
            HttpResponseMessage connectionrolesresponse;
            HttpRequestMessage deleteConnectionRequest;
            string connectionssearch = "";

            try
            {
                connectionssearch = d365client.BaseAddress.ToString() + "v9.0/connections?$select=connectionid&$filter=(_record1id_value eq " + Id2.ToString() + " and _record2id_value eq " + Id1.ToString() + ") or (_record1id_value eq " + Id1.ToString() + " and _record2id_value eq " + Id2.ToString() + ")";
                HttpRequestMessage createRequest = new HttpRequestMessage(new HttpMethod("GET"), connectionssearch);
                connectionrolesresponse = await d365client.SendAsync(createRequest);
                jsonconnectionstoremove = JObject.Parse(connectionrolesresponse.Content.ReadAsStringAsync().Result);
                jsonconnectiontoremove = jsonconnectionstoremove.Children().Last();
                jsonconnectiontoremove = jsonconnectiontoremove.Children().First();
                foreach (JObject jsonindividualconnectiontoremove in jsonconnectiontoremove)
                {
                    foreach (JProperty property in jsonindividualconnectiontoremove.Children<JProperty>())
                    {
                        switch (property.Name.ToLower())
                        {
                            case "connectionid":
                                connectionssearch = d365client.BaseAddress.ToString() + "v9.0/connections(" + property.Value.ToString() + ")";
                                deleteConnectionRequest = new HttpRequestMessage(HttpMethod.Delete, connectionssearch);
                                connectionrolesresponse = await d365client.SendAsync(deleteConnectionRequest);
                                break;
                        }
                    }
                }
            }
            catch
            {
                Console.WriteLine("Error");
            }

            //  Z.  Close the database connection now.
            return true;

        }
        private HttpClient D365Connection(out DateTime revalidatetime)
        {
            DateTimeOffset timetorevalidate;
            var resource = ConfigurationManager.AppSettings["MSFTAzureD365Address"];
            var clientId = ConfigurationManager.AppSettings["MSFTAzureD365ClientId"];
            var redirectUri = ConfigurationManager.AppSettings["MSFTAzureD365Redirect"];
            var clientSecret = ConfigurationManager.AppSettings["MSFTAzureD365Secret"];
            var authorityUrl = ConfigurationManager.AppSettings["MSFTAzureD365Authority"];
            var authBuilder = ConfidentialClientApplicationBuilder.Create(clientId)
                             .WithRedirectUri(redirectUri)
                             .WithClientSecret(clientSecret)
                             .WithAuthority(authorityUrl)
                             .Build();
            var scope = resource + "/.default";
            string[] scopes = { scope };
            Microsoft.Identity.Client.AuthenticationResult token = authBuilder.AcquireTokenForClient(scopes).ExecuteAsync().Result;

            var client = new HttpClient
            {
                BaseAddress = new Uri(resource + "/api/data/"),
                Timeout = new TimeSpan(0, 2, 0)
            };

            HttpRequestHeaders headers = client.DefaultRequestHeaders;
            headers.Authorization = new AuthenticationHeaderValue("Bearer", token.AccessToken);
            headers.Add("OData-MaxVersion", "4.0");
            headers.Add("OData-Version", "4.0");
            headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            revalidatetime = token.ExpiresOn.DateTime;

            return client;
        }

    }
}
