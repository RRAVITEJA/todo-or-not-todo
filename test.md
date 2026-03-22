
TODO:
- Android mobile app release
- Test mobile app
- Send sample documents
- Reporting
    - Make All lambda functions tenant specific
    - Add edit & delete report config/definition API, similar to create-report-definition-lambda
    - UI
    - Add other entities to the reporting 
        - Documents
        - Document requests
        - Signature requests
        - Affiliations
        - Enrollments
        - Tasks
        - Workflows
        - Facilites


Reporting lambdas
- arn:aws:lambda:us-east-2:058264370635:function:section-metadata-redshift-fn
Get all data fields for reporting

- arn:aws:lambda:us-east-2:058264370635:function:create-report-definition-lambda
store the report configuration in a report table, when user picks all the data fields he wants to see in a report, we store these report configuration/difinition a redshift table name report.this lambda takes the list of data fields the user selected as input.

- arn:aws:lambda:us-east-2:058264370635:function:create-materalized-view
Creates a materalized view of from report definition, this lambda takes the report id as input, pull the report definition from the reports table above and then creates a view from the report config.

- arn:aws:lambda:us-east-2:058264370635:function:redshift-report-data-fn
list all report definitions

- arn:aws:lambda:us-east-2:058264370635:function:get-report-data-lambda
get data from the materalized view.input reprot id


