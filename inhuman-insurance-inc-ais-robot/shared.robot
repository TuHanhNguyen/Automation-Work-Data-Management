*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 shared settings and code

Library             RPA.Robocorp.WorkItems
Library             RPA.JSON
Library             RPA.HTTP


*** Variables ***
${WORK_ITEM_NAME}=      traffic_data
