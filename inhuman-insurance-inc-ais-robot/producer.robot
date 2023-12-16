*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Produces traffic data work items.

Library             RPA.HTTP
Library             RPA.JSON
Library             RPA.Tables
Library             Collections
Library             RPA.Robocorp.WorkItems


*** Variables ***
${JSON_PATH}=       ${OUTPUT_DIR}${/}traffic.json
${COUNTRY}=         SpatialDim
${YEAR}=            TimeDim
${RATE}=            NumericValue


*** Tasks ***
Produce traffic data work items
    # Download the traffic data
    ${traffic_data_table}=    Transform traffic data into a Table
    ${filtered_data_table}=    Filter and sort traffic data    ${traffic_data_table}
    Write table to CSV    ${filtered_data_table}    ${OUTPUT_DIR}${/}filtered_trafficdata.csv    header=True
    ${filterd_data_table_lastest_by_country}=    Get lastest available traffic data for each country
    ...    ${filtered_data_table}
    ${pay_loads}=    Create work item payloads    ${filterd_data_table_lastest_by_country}
    Save work item payloads    ${pay_loads}

    Log    Done.


*** Keywords ***
Save work item payloads
    [Arguments]    ${pay_loads}
    FOR    ${row}    IN    @{pay_loads}
        Log    ${row}
        Save work item payload ${row}
    END

Save work item payload
    [Arguments]    ${row}
    ${Variables}=    Create Dictionary    traffic_data=${row}
    Create Output Work Item    ${Variables}    save=True

Create work item payloads
    [Arguments]    ${arg}
    ${payloads}=    Create List
    FOR    ${rows}    IN    @{arg}
        Log    ${rows}
        ${payload}=    Create Dictionary
        ...    country=${rows}[SpacialDim]
        ...    year=${rows}[${YEAR}]
        ...    rate=${rows}[${RATE}]
        Append To List    ${payloads}    ${payload}
    END
    RETURN    ${payloads}

Get lastest available traffic data for each country
    [Arguments]    ${table}
    ${country}=    Set Variable    ${COUNTRY}
    ${table_sorted_by_group}=    Group Table By Column    ${table}    ${country}
    ${table_lastest_data_by_country}=    Create List
    FOR    ${group}    IN    @{table_sorted_by_group}
        ${first_row}=    Pop Table Row    ${group}
        Append To List    ${table_lastest_data_by_country}
    END
    RETURN    ${table_lastest_data_by_country}

Filter and sort traffic data
    [Arguments]    ${data_table}
    ${max_rate}=    Set Variable    ${5.0}
    ${rate_key}=    Set Variable    ${RATE}
    ${gender_key}=    Set Variable    Dim1
    ${both_genders}=    Set Variable    BTSX
    ${year_key}=    Set Variable    ${YEAR}
    Filter Table By Column    ${data_table}    ${rate_key}    <    ${max_rate}
    Filter Table By Column    ${data_table}    ${gender_key}    ==    ${both_genders}
    Sort Table By Column    ${data_table}    ${year_key}
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
