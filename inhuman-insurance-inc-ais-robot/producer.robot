*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Produces traffic data work items.

Library             RPA.JSON
Library             RPA.Tables
Library             Collections
Resource            shared.robot


*** Variables ***
${JSON_PATH}=       ${OUTPUT_DIR}${/}traffic.json
${COUNTRY}=         SpatialDim
${GENDER}=          Dim1
${YEAR}=            TimeDim
${RATE}=            NumericValue


*** Tasks ***
Produce traffic data work items
    Download the traffic data
    ${traffic_data_table}=    Transform traffic data into a Table
    ${filtered_data_table}=    Filter and sort traffic data    ${traffic_data_table}
    Write table to CSV    ${filtered_data_table}    ${OUTPUT_DIR}${/}filtered_trafficdata.csv    header=True
    ${filterd_data_table_lastest_by_country}=    Get lastest available traffic data for each country
    ...    ${filtered_data_table}
    ${pay_loads}=    Create work item payloads    ${filterd_data_table_lastest_by_country}
    Save work item payloads    ${pay_loads}


*** Keywords ***
Save work item payloads
    [Arguments]    ${payloads}
    FOR    ${payload}    IN    @{payloads}
        Save work item payload    ${payload}
    END

Save work item payload
    [Arguments]    ${payload}
    ${variables}=    Create Dictionary    ${WORK_ITEM_NAME}=${payload}
    Create Output Work Item    ${variables}    save=True

Create work item payloads
    [Arguments]    ${arg}
    ${payloads}=    Create List
    FOR    ${rows}    IN    @{arg}
        Log    ${rows}
        ${payload}=    Create Dictionary
        ...    country=${rows}[${COUNTRY}]
        ...    year=${rows}[${YEAR}]
        ...    rate=${rows}[${RATE}]
        Append To List    ${payloads}    ${payload}
    END
    RETURN    ${payloads}

Get lastest available traffic data for each country
    [Arguments]    ${table}
    ${table_sorted_by_group}=    Group Table By Column    ${table}    ${COUNTRY}
    ${table_lastest_data_by_country}=    Create List
    FOR    ${group}    IN    @{table_sorted_by_group}
        ${first_row}=    Pop Table Row    ${group}
        Append To List    ${table_lastest_data_by_country}    ${first_row}
    END
    RETURN    ${table_lastest_data_by_country}

Filter and sort traffic data
    [Arguments]    ${data_table}
    ${max_rate}=    Set Variable    ${5.0}
    ${both_genders}=    Set Variable    BTSX
    Filter Table By Column    ${data_table}    ${RATE}    <    ${max_rate}
    Filter Table By Column    ${data_table}    ${GENDER}    ==    ${both_genders}
    Sort Table By Column    ${data_table}    ${YEAR}
    RETURN    ${data_table}

Download the traffic data
    Download
    ...    https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json
    ...    ${JSON_PATH}
    ...    overwrite=True

Transform traffic data into a Table
    ${json}=    Load JSON from file    ${JSON_PATH}
    ${table}=    Create Table    ${json}[value]
    Write table to CSV    ${table}    ${OUTPUT_DIR}${/}trafficdata.csv    header=True
    RETURN    ${table}
