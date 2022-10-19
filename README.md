# REST API for Snowpipe Streaming
This repository is an example of how to build a REST API in AWS for
Snowflake's Snowpipe Streaming.

Note: at this point (October 18, 2022), Snowpipe Streaming is still in 
Private Preview.

The purpose is to allow for applications to put data to a REST endpoint
and that data will be loaded into Snowflake in a low-latency fashion (versus
using batch/bulk load methods). This is not always the best way to load data
into Snowflake, though, and if you need to load large sets of data you should
look at bulk methods including COPY, EXTERNAL TABLEs, and Snowpipe.

The example herein is implemented in Java as that is the only language that
Snowpipe Streaming supports.

This example creates an API that will allow you to put data to any table
in any schema in any database. Of course, this is restricted to the RBAC
permissions of the role that Snowpipe Streaming uses to connect. But the
API endpoint uses path variables for any database, any schema, and any 
table. In reality, you would restrict the API to specific tables. A simple
way to do that is to copy the `path` in the `template.yaml` and replace
`/snowpipe/{database}/{schema}/{table}` with the table of interest, e.g.,
`/snowpipe/mydb/myschema/mytable`. You can copy that block of YAML as
many times as you like, providing different paths. Additionally, you 
can wildcard things, too. For example, to allow any table in the `myschema`
schema in the `mydb` database you could supply `/snowpipe/mydb/{schema}/{table}`.

# Setup
This example uses the AWS Serverless Application Model (SAM). For details
on setting up AWS SAM, see the [documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html).

This example will connect to Snowflake and perform the ingestion operations
to the specified table(s). In order to support this, we need to create a
role with access to the tables of interest, a user who has been granted
that role and has an SSH keypair. See the Snowflake [documentation](https://docs.snowflake.com)
for more details.

The stack that is created for this example uses a secret stored in 
Amazon Secrets Manager. To create the secret in Secrets Manager, follow
the instructions [here](https://docs.aws.amazon.com/secretsmanager/latest/userguide/create_secret.html).
The secret needs 4 fields:
* `url` - the Snowflake Account URL (e.g., https://myacct.snowflakecomputing.com)
* `role` - the Snowflake role with access to the tables of interest
* `user` - the Snowflake user in that account that has been granted the `role`
* `private_key` - the SSH private key associated with the `user` (single line, without header)

The JSON value of the secret should look like the following (with your values substituted):
```
{
    "url": "<SNOWFLAKE URL>",
    "role": "<SNOWFLAKE ROLE>",
    "user": "<SNOWFLAKE USER>",
    "private_key": "<SNOWFLAKE PRIVATE KEY>"
}
```

# Creating the API in AWS
The stack will create the following resources:
* An IAM role for the Lambda functions to run that have access to the secret for Snowflake access
* An AWS Lambda function that will do the work to write to Snowflake over Snowpipe Streaming
* An AWS Lambda function to protect the API endpoint with username/password authentication
* Permissions for API Gateway to invoke the Lambda functions
* An API Gateway HTTP API that exposes a PUT endpoint that is wired to the Lambda function

This stack takes the following parameters:
* `prefix` - a prefix that will be prepended to all resources created (to make them easy to find and avoid name collisions)
* `SnowflakeSecret` - the ARN of the secret in Secrets Manager
* `APIUsername` - the username to protect the API endpoint
* `APIPassword` - the password to protect the API endpiont

The output is the root of the API that is stood up, e.g., :
```
https://UUIDSTRING.execute-api.REGION.amazonaws.com/PREFIX/snowpipe/
```

To stand this up in AWS, first we use AWS SAM to build the stack using

```
sam build --use-container
```

Once that complete successfully, we can deploy to AWS using

```
sam deploy --capabilities CAPABILITY_NAMED_IAM --guided
```

This command will interactively prompt you for the inputs. In addition to 
the parameters above, AWS SAM will ask for a stack name (you can choose whatever
you want) and which AWS region. After providing the 4 inputs above, AWS SAM
will also ask a few additional yes/no questions - you can use the defaults.

Once complete, AWS SAM will output the root as above.

# Using the API
The API takes data as a JSON array of objects, one object per row to insert
into Snowflake. The data is landed in Snowflake in the various columns, where
the JSON object's fields are mapped directly to the table's columns by matching
the field name to the column name. The array can have 0 or more objects to insert.

In order to use the API, you can issue PUT commands to 
```
{API_ROOT}/{database}/{schema}/{table}
```

For example, to put the simple `{"a": 1, "b": "one"}` row into the `mydb.myschema.mytable`
table using `curl`, you could do:
```
curl -X PUT -u myuser:mypwd -H "Content-Type: application/json" -d '[{"a": 1, "b": "one"}]' "https://ABCDEFG.execute-api.us-west-2.amazonaws.com/mystack/snowpipe/mydb/myschema/myuser"
```

Where the output of the AWS SAM deploy command is `https://ABCDEFG.execute-api.us-west-2.amazonaws.com/mystack/snowpipe/`
and the API is protected with the username `myuser` and the password `mypwd`.

Multiple records could be loaded at the same time, as well:
```
curl -X PUT -u myuser:mypwd -H "Content-Type: application/json" -d '[{"a": 1, "b": "one"}, {"a": 2, "b": "two"}, {"a": 3, "b": "three"}]' "https://ABCDEFG.execute-api.us-west-2.amazonaws.com/mystack/snowpipe/mydb/myschema/myuser"
```

# Cleaning Up
To delete the AWS stack, you can run
```
sam delete
```

Remember to delete the secret in AWS Secrets Manager that we created outside of AWS SAM.
