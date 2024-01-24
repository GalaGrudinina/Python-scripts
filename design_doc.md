# Design Document

## The purpose of this design

To visualize every stage in the pipeline execution process in order to

## What data will be collected

Daily  logs including the following:

- A ...

## What transformations will be applied

There is no transformations to be done from the engineering side

## Pipeline execution frequency time

Monthly

## How to implemnt changes to scripts

- Permission:
- Risks when changing the script:
- How to make changes: need to make PR
- Who decide reviewers:
A. assigned automatically to anyone in  team (GHE)
B. the person who makes commits from the given list of team members(Bitbucket)
- Who approves changes(merge->deployment): one of the mentioned above assignees

## Monitoring

- Logs are collected and stored
- Individual Slack channel is created for each script
- The person who is on User support/ Pagerduty rotation keeps an eye on the pipelines execution

## Links

|System | Link|
| -------------          |----------         |
|Issue| |
|Bitbucket repo ||


## Data Flow Diagram

```mermaid
graph LR;
    A(A) --> B((B));
    B --> E[|borders:tb|C];
    E --> H[|borders:tb|D];
    H--> L[F];
```

## Sequence Diagram

``` mermaid
sequenceDiagram
autonumber
loop certain hours
S--> G: pushes metrics
end
S->> G: requests the performance metrics
G -->>S:sends the response with metrics
S ->>T : requests the data about instance
T-->>St:sends the response with data
J ->> S : executes
J ->> J : runs
S ->> D:  pushes collected data
D ->> D: stores in report.csv
```