# Mainframe_DataInsights_change_data_reconcilition

If your organization is hosting business-critical data in a mainframe environment, gaining insights from that data is crucial for driving growth and innovation. By unlocking mainframe data, you can build faster, secure, and scalable business intelligence to accelerate data-driven decision-making, growth, and innovation in the Amazon Web Services (AWS) Cloud.

This pattern presents a solution for generating business insights and creating sharable narratives from mainframe data [in IBM Db2 for z/OS tables] by using AWS Mainframe Modernization Data Replication with Precisely and Amazon Q in QuickSight. Mainframe data changes are stream to Amazon Managed Streaming for Apache Kafka (MSK) topic using AWS Mainframe Modernization Data replication with Precisely. Using Amazon Redshift Streaming Ingestion, Amazon MSK topic data stream are stored in Amazon Redshift Serverless data warehouse tables for analytics in Amazon QuickSight. 

After the data is available in Amazon QuickSight, you can use natural language prompts with Amazon Q in QuickSight to create summaries of the data, ask questions, and generate data stories. You don't have to write SQL queries or learn a business intelligence (BI) tool.

The code in this repository is for the Stored Procedure in Amazon Redshift. This stored procedure does the mainframe change data (insert/update/deletes) reconciliation into a Amazon Redshift tables from the Amazon MSK. These Amazon Redshift tables serves as data analytics source for Amazon QuickSight.

## Target architecture diagram

![image](https://github.com/user-attachments/assets/96ccea82-f46b-46d8-b86c-f1a2ae71e3e3)

### Code in this repository is for Stored Procedure belong to #6 in the architecture diagram.

For prerequisites and instructions for using this AWS Prescriptive Guidance pattern, see [Unlock mainframe data using AWS Mainframe Modernization Data Replication with Precisely to generate data insights with Amazon Q in QuickSight](https://apg-library.amazonaws.com/content-viewer/author/18e72bcb-1b9a-406a-8220-83aca7743ad2).


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

