*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Produces traffic data work items.

Resource            shared.robot


*** Tasks ***
Consume traffic data work items
    For Each Input Work Item    Process traffic data


*** Keywords ***
Process traffic data
    ${payload}=    Get Work Item Payload
    ${traffic_data}=    Set Variable    ${payload}[${WORK_ITEM_NAME}]
    Log    ${payload}
    Log    ${traffic_data}
    ${valid}=    Validate traffic data    ${traffic_data}
    IF    ${valid}    Post traffic data to sales system    ${traffic_data}

Post traffic data to sales system
    [Arguments]    ${traffic_data}
    ${status}    ${return}=    Run Keyword And Ignore Error
    ...    POST
    ...    https://robocorp.com/inhuman-insurance-inc/sales-system-api
    ...    json=${traffic_data}

Validate traffic data
    [Arguments]    ${traffic_data}
    ${country}=    Get value from JSON    ${traffic_data}    $.country
    ${valid}=    Evaluate    len("${country}") == 3
    RETURN    ${valid}
